package com.wepay.waltz.tools.cluster;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.InternalRpcClient;
import com.wepay.waltz.common.metadata.ConnectionMetadata;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.metadata.StoreParams;
import com.wepay.waltz.common.metadata.ReplicaAssignments;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.common.metadata.ReplicaState;
import com.wepay.waltz.common.metadata.PartitionMetadata;
import com.wepay.waltz.common.metadata.PartitionMetadataSerializer;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.DynamicPartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.internal.PartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.internal.ServerDescriptor;
import com.wepay.zktools.clustermgr.internal.PartitionAssignment;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import io.netty.handler.ssl.SslContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * ClusterCli is a tool for interacting with the Waltz Cluster.
 */
public final class ClusterCli extends SubcommandCli {

    private ClusterCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
            new Subcommand(CheckConnectivity.NAME, CheckConnectivity.DESCRIPTION, CheckConnectivity::new),
            new Subcommand(Verify.NAME, Verify.DESCRIPTION, Verify::new)
        ));
    }

    /**
     * The {@code CheckConnectivity} command checks the connectivity
     * 1. of the server nodes, and
     * 2. of the storage nodes from each server node
     * within the Waltz Cluster.
     */
    private static final class CheckConnectivity extends Cli {
        private static final String NAME = "check-connectivity";
        private static final String DESCRIPTION = "Checks the connectivity of the cluster.";

        private final PartitionAssignmentPolicy partitionAssignmentPolicy = new DynamicPartitionAssignmentPolicy();

        private CheckConnectivity(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option cliCfgOption = Option.builder("c")
                .longOpt("cli-config-path")
                .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config")
                .hasArg()
                .build();
            cliCfgOption.setRequired(true);

            options.addOption(cliCfgOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            ZooKeeperClient zkClient = null;
            InternalRpcClient rpcClient = null;
            try {
                String cliConfigPath = cmd.getOptionValue("cli-config-path");
                CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
                String zookeeperHostPorts = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
                String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
                int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);

                ClusterManager clusterManager = new ClusterManagerImpl(zkClient, root, partitionAssignmentPolicy);
                Set<Endpoint> serverEndpoints =
                    clusterManager.serverDescriptors()
                        .stream()
                        .map(serverDescriptor -> serverDescriptor.endpoint)
                        .collect(Collectors.toSet());

                DummyTxnCallbacks callbacks = new DummyTxnCallbacks();
                rpcClient = new InternalRpcClient(sslContext, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS,
                    callbacks);

                Map<Endpoint, Map<String, Boolean>> connectivityStatusMap =
                    rpcClient.checkServerConnections(serverEndpoints).get();

                for (Endpoint endpoint : serverEndpoints) {
                    Map<String, Boolean> connectivityStatus = connectivityStatusMap.get(endpoint);
                    System.out.println("Connectivity status of " + endpoint + " is: "
                        + ((connectivityStatus == null) ? "UNREACHABLE" : connectivityStatus.toString())
                    );
                }
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to check topology of the cluster. %n%s",
                    e.getMessage()));
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }

                if (rpcClient != null) {
                    rpcClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code verify} command checks:
     * 1. Zookeeper partition assignment metadata is valid
     * 2. The partition assignment of actual servers matches the one on Zookeeper
     * 3. All of the servers are reachable and each of those can reach its replica storage nodes.
     * 4. The partition assignment of storage nodes matches the one on Zookeeper and also the corresponding
     * partitions quorum.
     */
    private static final class Verify extends Cli {
        private static final String NAME = "verify";
        private static final String DESCRIPTION = "Validates if partition(s) is handled by some server";

        private final PartitionAssignmentPolicy partitionAssignmentPolicy = new DynamicPartitionAssignmentPolicy();
        private final StringBuilder outputBuilder = new StringBuilder();

        private long timeoutInSeconds = 10;

        private Verify(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {

            Option cliCfgOption = Option.builder("c")
                .longOpt("cli-config-path")
                .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config")
                .hasArg()
                .build();

            Option partitionOption = Option.builder("p")
                .longOpt("partition")
                .desc("Partition to validate. If not specified, all partitions in cluster are validated")
                .hasArg()
                .build();

            Option timeoutOption = Option.builder("t")
                .longOpt("timeout")
                .desc("Specify the timeout in seconds. If not specified, default value is considered.")
                .hasArg()
                .build();

            Option loggerOutputOption = Option.builder("l")
                .longOpt("logger-as-output")
                .desc("Cli output will be sent to logger instead of standard output")
                .build();

            cliCfgOption.setRequired(true);
            partitionOption.setRequired(false);
            timeoutOption.setRequired(false);
            loggerOutputOption.setRequired(false);

            options.addOption(cliCfgOption);
            options.addOption(partitionOption);
            options.addOption(timeoutOption);
            options.addOption(loggerOutputOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            ZooKeeperClient zkClient = null;
            InternalRpcClient rpcClient = null;
            List<PartitionValidationResults> partitionsValidationResultList = new ArrayList<>();
            int partitionId = -1;
            boolean loggerAsOutput = cmd.hasOption("logger-as-output");

            try {
                String cliConfigPath = cmd.getOptionValue("cli-config-path");
                CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
                String zookeeperHostPorts = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
                ZNode zkRoot = new ZNode((String) cliConfig.get(CliConfig.CLUSTER_ROOT));
                ZNode storeRoot = new ZNode(zkRoot, StoreMetadata.STORE_ZNODE_NAME);
                int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, storeRoot);
                ClusterManager clusterManager = new ClusterManagerImpl(zkClient, zkRoot, partitionAssignmentPolicy);
                int numPartitions = clusterManager.numPartitions();

                if (cmd.hasOption("partition")) {
                    partitionId = Integer.parseInt(cmd.getOptionValue("partition"));
                    if ((partitionId < 0) || (partitionId >= numPartitions)) {
                        throw new IllegalArgumentException("Partition " + partitionId + " is not valid.");
                    }
                }
                if (cmd.hasOption("timeout")) {
                    timeoutInSeconds = Integer.parseInt(cmd.getOptionValue("timeout"));
                }

                for (int pId = 0; pId < clusterManager.numPartitions(); pId++) {
                    partitionsValidationResultList.add(new PartitionValidationResults(pId));
                }

                // Step1: Validate all partitions on zk
                buildZookeeperPartitionAssignmentsValidation(clusterManager, partitionsValidationResultList);

                // Step2: Build zk and servers partition assignment consistency validations
                rpcClient = new InternalRpcClient(sslContext, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS,
                    new DummyTxnCallbacks());

                CompletableFuture<Void> consistencyValidationFuture =
                    buildServersZKPartitionAssignmentsConsistencyValidation(
                        rpcClient, clusterManager, partitionsValidationResultList
                    );

                // Step3: Build Server-Storage connectivity validations
                CompletableFuture<Void> connectivityValidationFuture =
                    buildServerStorageConnectivityValidation(
                        rpcClient,
                        clusterManager,
                        storeMetadata.getReplicaAssignments(),
                        partitionsValidationResultList
                    );

                // Step4: Validate zk and storage partition assignment consistency and Quorum
                Map<Integer, Set<String>> zkPartitionToStorageNodeMap =
                    getZkPartitionToStorageNodeMapping(storeMetadata);

                Map<String, Integer> storageConnections = getStorageConnections(storeMetadata);

                Map<Integer, Map<String, Boolean>> partitionToStorageStatusMap =
                    getPartitionStatusFromStorage(storeMetadata, storageConnections, sslContext);

                buildStoragePartitionValidationResult(numPartitions, storageConnections, zkPartitionToStorageNodeMap,
                    partitionToStorageStatusMap, partitionsValidationResultList);

                // Step5: Validate ZK store and server generation consistency
                ZNode partitionRoot = new ZNode(storeRoot, StoreMetadata.PARTITION_ZNODE_NAME);

                CompletableFuture<Void> generationValidationFuture =
                    buildServersZKPartitionMetadataCorrectnessValidation(
                        rpcClient, clusterManager, partitionRoot, zkClient, partitionsValidationResultList
                    );

                // Step6: Validate recovery is completed and replica states updated in ZK
                buildStorageRecoveryCompleteValidationResult(numPartitions, partitionRoot,
                    zkPartitionToStorageNodeMap, zkClient, partitionsValidationResultList);

                // Update validation result for remaining partitions if not already updated.
                updateRemainingValidationResults(consistencyValidationFuture,
                    ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                    partitionsValidationResultList);
                updateRemainingValidationResults(connectivityValidationFuture,
                    ValidationResult.ValidationType.SERVER_STORAGE_CONNECTIVITY,
                    partitionsValidationResultList);
                updateRemainingValidationResults(generationValidationFuture,
                    ValidationResult.ValidationType.PARTITION_GENERATION_ZK_SERVER_CONSISTENCY,
                    partitionsValidationResultList);

                // Verify all results or just one partition if "partition" option is present
                verifyValidation(partitionsValidationResultList, partitionId);

            } catch (RuntimeException e) {
                throw new SubCommandFailedException(String.format("Failed to verify cluster. %n%s",
                    e.getMessage()));
            } catch (Exception  e) {
                throw new SubCommandFailedException(String.format("Failed to verify cluster. %n%s",
                        e.getMessage()));
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }

                if (rpcClient != null) {
                    rpcClient.close();
                }

                if (loggerAsOutput) {
                    Logger logger = Logging.getLogger(Verify.class);
                    String[] splitParagraphs = outputBuilder.toString().split("\n(?=[^\\s])");
                    for (String paragraph : splitParagraphs) {
                        logger.info(paragraph);
                    }
                } else {
                    System.out.println(outputBuilder);
                }
            }
        }

        private void updateRemainingValidationResults(CompletableFuture<Void> future,
                                                      ValidationResult.ValidationType validationType,
                                                      List<PartitionValidationResults> partitionsValidationResultList) throws Exception {
            String error = "No Server Endpoint found.";

            try {
                future.get(timeoutInSeconds, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException exception) {
                error = exception.getMessage();
            }

            failMissingValidationResults(
                partitionsValidationResultList,
                validationType,
                error
            );
        }

        private void failMissingValidationResults(List<PartitionValidationResults> partitionsValidationResultsList,
                                                  ValidationResult.ValidationType validationType,
                                                  String failureMessage) {
            for (PartitionValidationResults results : partitionsValidationResultsList) {
                if (!results.validationResultsMap.containsKey(validationType)) {
                    ValidationResult validationResult =
                        new ValidationResult(validationType, ValidationResult.Status.FAILURE, failureMessage);
                    results.validationResultsMap.put(validationType, validationResult);
                }
            }
        }

        private void buildStoragePartitionValidationResult(int numPartitions,
                                                           Map<String, Integer> storageConnections,
                                                           Map<Integer, Set<String>> zkPartitionToStorageNodeMap,
                                                           Map<Integer, Map<String, Boolean>> partitionToStorageStatusMap,
                                                           List<PartitionValidationResults> partitionValidationResultsList) {
            for (int id = 0; id < numPartitions; id++) {
                ValidationResult partitionAssignmentValidationResult;
                ValidationResult quorumValidationResult;
                PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(id);

                // Verity zk and storage node partition assignment consistency for a partition Id.
                Set<String> zkStorageNodeMapPerId = zkPartitionToStorageNodeMap.getOrDefault(id,
                    new HashSet<>());
                Map<String, Boolean> storageStatusMapPerId = partitionToStorageStatusMap.getOrDefault(id,
                    new HashMap<>());
                if ((zkStorageNodeMapPerId.size() != storageStatusMapPerId.size()) || (!zkStorageNodeMapPerId.equals(storageStatusMapPerId.keySet()))) {
                    partitionAssignmentValidationResult = new ValidationResult(
                        ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_STORAGE_CONSISTENCY,
                        ValidationResult.Status.FAILURE, "Error: ZooKeeper and StorageNode Partition Assignment "
                        + "mismatch");
                } else {
                    partitionAssignmentValidationResult = new ValidationResult(
                        ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_STORAGE_CONSISTENCY,
                        ValidationResult.Status.SUCCESS, "");
                }
                partitionValidationResults.validationResultsMap.put(partitionAssignmentValidationResult.type,
                    partitionAssignmentValidationResult);

                // Verify quorum
                List<String> quorumList = new ArrayList<>();
                Map<String, Boolean> storageReadWriteStatusMap = partitionToStorageStatusMap.getOrDefault(id, new HashMap<>());
                storageReadWriteStatusMap.forEach((storageConnectString, status) -> {
                    if (status) {
                        quorumList.add(storageConnectString);
                    }
                });

                if (quorumList.size() <= (storageConnections.size() / 2)) {
                    quorumValidationResult = new ValidationResult(
                        ValidationResult.ValidationType.PARTITION_QUORUM_STATUS, ValidationResult.Status.FAILURE,
                        "Error: Quorum is not achieved");
                } else {
                    quorumValidationResult =
                        new ValidationResult(ValidationResult.ValidationType.PARTITION_QUORUM_STATUS,
                            ValidationResult.Status.SUCCESS, "");
                }
                partitionValidationResults.validationResultsMap.put(quorumValidationResult.type,
                    quorumValidationResult);
            }
        }

        /**
         * Builds REPLICA_RECOVERY_STATUS {@code PartitionValidationResults} for all the replicas in the
         * cluster.
         * The validation result is set to success if and only if:
         * 1. The set of replicas in replica assignments is the same as the set of replicas in replica states.
         * 2. For each replica, the session id in the replica states equals the session id in partitionMetadata,
         * and closingHighWaterMark is UNRESOLVED.
         *
         * @param numPartitions Total number of partitions in the cluster.
         * @param partitionRoot ZNode used to fetch replica states for each partition.
         * @param zkPartitionToStorageNodeMap Map of partitionId to set of storageConnectString.
         * @param zkClient ZooKeeper Client used to fetch partition metadata.
         * @param partitionsValidationResultsList a {@code List<PartitionValidationResults>} that contains all the
         *                                       validation results.
         */
        private void buildStorageRecoveryCompleteValidationResult(int numPartitions,
                                                                  ZNode partitionRoot,
                                                                  Map<Integer, Set<String>> zkPartitionToStorageNodeMap,
                                                                  ZooKeeperClient zkClient,
                                                                  List<PartitionValidationResults> partitionsValidationResultsList) {
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                Set<String> storageNodeSet = zkPartitionToStorageNodeMap.get(partitionId);
                String error = "";
                ZNode zNode = new ZNode(partitionRoot, Integer.toString(partitionId));
                Map<ReplicaId, ReplicaState> replicaState;
                Long sessionId;

                try {
                    PartitionMetadata partitionMetadata = zkClient.getData(zNode, PartitionMetadataSerializer.INSTANCE).value;
                    replicaState = partitionMetadata.replicaStates;
                    sessionId = partitionMetadata.sessionId;
                } catch (KeeperException | ZooKeeperClientException e) {
                    replicaState = Collections.emptyMap();
                    sessionId = null;
                    error = "Cannot obtain replica states";
                }

                for (Map.Entry<ReplicaId, ReplicaState> entry : replicaState.entrySet()) {
                    String storageNodeConnectString = entry.getKey().storageNodeConnectString;
                    if (!storageNodeSet.contains(storageNodeConnectString)) {
                        error = error.concat(System.lineSeparator())
                            .concat(String.format("Storage %s replica info missing in replica assignments", storageNodeConnectString));
                        continue;
                    }
                    storageNodeSet.remove(storageNodeConnectString);

                    boolean recoveryComplete =
                        (sessionId != null)
                        && (entry.getValue().sessionId == sessionId)
                        && (entry.getValue().closingHighWaterMark == ReplicaState.UNRESOLVED);
                    if (!recoveryComplete) {
                        error = error.concat(System.lineSeparator())
                            .concat(String.format("Storage %s recovery not completed", storageNodeConnectString));
                    }
                }

                if (storageNodeSet.isEmpty()) {
                    zkPartitionToStorageNodeMap.remove(partitionId);
                }

                ValidationResult validationResult = new ValidationResult(
                    ValidationResult.ValidationType.REPLICA_RECOVERY_STATUS,
                    ("".equals(error)) ? ValidationResult.Status.SUCCESS : ValidationResult.Status.FAILURE,
                    error);
                PartitionValidationResults partitionValidationResults = partitionsValidationResultsList.get(partitionId);
                partitionValidationResults.validationResultsMap.put(validationResult.type, validationResult);
            }

            // For each remaining storageNode, there is only corresponding info in replica assignments and not in replica states
            zkPartitionToStorageNodeMap.forEach((partitionId, storageNodeSet) -> {
                PartitionValidationResults partitionValidationResults = partitionsValidationResultsList.get(partitionId);
                ValidationResult validationResult = partitionValidationResults.validationResultsMap
                    .get(ValidationResult.ValidationType.REPLICA_RECOVERY_STATUS);

                String error = validationResult == null ? "" : validationResult.error;
                for (String storageNode : storageNodeSet) {
                    error = error.concat(System.lineSeparator())
                        .concat(String.format("Storage %s replica info missing in replica states", storageNode));
                }

                validationResult = new ValidationResult(
                    ValidationResult.ValidationType.REPLICA_RECOVERY_STATUS,
                    ValidationResult.Status.FAILURE,
                    error
                );
                partitionValidationResults.validationResultsMap.put(validationResult.type, validationResult);
            });

            failMissingValidationResults(
                partitionsValidationResultsList,
                ValidationResult.ValidationType.REPLICA_RECOVERY_STATUS,
                "Cannot obtain replica states"
            );
        }

        private StorageAdminClient openStorageAdminClient(String storageHost, Integer storageAdminPort,
                                                          SslContext sslContext, StoreMetadata storeMetadata) throws Exception {
            StoreParams storeParams = storeMetadata.getStoreParams();
            StorageAdminClient storageAdminClient = null;
            try {
                storageAdminClient = new StorageAdminClient(storageHost, storageAdminPort, sslContext,
                    storeParams.key, storeParams.numPartitions);
                storageAdminClient.open();

            } catch (Exception e) {
                // Do Nothing.
            }
            return storageAdminClient;
        }

        @SuppressWarnings("unchecked")
        private Map<Integer, Map<String, Boolean>> getPartitionStatusFromStorage(StoreMetadata storeMetadata,
                                                                                 Map<String, Integer> storageConnections,
                                                                                 SslContext sslContext) throws Exception {
            Map<Integer, Map<String, Boolean>> partitionToStorageStatusMap = new HashMap<>();
            List<StorageAdminClient> storageAdminClients = new ArrayList<>();
            List<CompletableFuture<Object>> futures = new ArrayList<>();

            for (Map.Entry<String, Integer> storageNodeConnection : storageConnections.entrySet()) {
                String[] storageHostAndPortArray = storageNodeConnection.getKey().split(":");
                String storageHost = storageHostAndPortArray[0];

                int storageAdminPort = storageNodeConnection.getValue();

                StorageAdminClient storageAdminClient = openStorageAdminClient(storageHost, storageAdminPort,
                    sslContext, storeMetadata);

                if (storageAdminClient.isValid()) {
                    CompletableFuture<Object> future = storageAdminClient.getAssignedPartitionStatus()
                        .whenComplete((obj, th) -> {
                            if (th == null) {
                                Map<Integer, Boolean> partitionStatusMap = (Map<Integer, Boolean>) obj;

                                synchronized (partitionToStorageStatusMap) {
                                    partitionStatusMap.forEach((partitionId, status) -> {
                                        partitionToStorageStatusMap.putIfAbsent(partitionId, new HashMap<>());
                                        partitionToStorageStatusMap.get(partitionId).put(storageNodeConnection.getKey(), status);
                                    });
                                }
                            }
                        });
                    storageAdminClients.add(storageAdminClient);
                    futures.add(future);
                }
            }

            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
                    .get(timeoutInSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
                // Do Nothing.
            } finally {
                for (StorageAdminClient storageAdminClient : storageAdminClients) {
                    storageAdminClient.close();
                }
            }

            return partitionToStorageStatusMap;
        }

        private Map<String, Integer> getStorageConnections(StoreMetadata storeMetadata) throws Exception {
            ConnectionMetadata connectionMetadata = storeMetadata.getConnectionMetadata();

            Map<String, Integer> storageConnections = new HashMap<>();
            if (connectionMetadata != null) {
                storageConnections = connectionMetadata.connections;
            }

            return storageConnections;
        }

        private Map<Integer, Set<String>> getZkPartitionToStorageNodeMapping(StoreMetadata storeMetadata) throws Exception {

            Map<Integer, Set<String>> zkPartitionToReplicaMap = new HashMap<>();

            storeMetadata.getReplicaAssignments().replicas.forEach((replicaConnection, partitionList) -> {
                for (Integer partitionId : partitionList) {
                    zkPartitionToReplicaMap.putIfAbsent(partitionId, new HashSet<>());
                    zkPartitionToReplicaMap.get(partitionId).add(replicaConnection);
                }
            });

            return zkPartitionToReplicaMap;
        }

        /**
         *  Verify validation results for all partitions.
         *  If validation failed, errors are printed
         *
         * @param results List containing the validation results for each partition
         * @param partitionId partitionId provided via the "partition" option, or else its -1
         * @return True if no error in validation. False otherwise
         */
        private boolean verifyValidation(List<PartitionValidationResults> results, int partitionId) {
            boolean success = true;
            if (partitionId == -1) {
                for (PartitionValidationResults partitionValidationResults : results) {
                    for (ValidationResult.ValidationType type : ValidationResult.ValidationType.values()) {
                        if (!verifyValidation(type, partitionValidationResults, partitionValidationResults.partitionId)) {
                            success = false;
                        }
                    }
                }
            } else {
                for (ValidationResult.ValidationType type : ValidationResult.ValidationType.values()) {
                    if (!verifyValidation(type, results.get(partitionId), partitionId)) {
                        success = false;
                    }
                }
            }
            return success;
        }

        /**
         * Verify a validation type for a specific partition from the list of validation results.
         * If validation failed, error is printed
         *
         * @param type Type of validation
         * @param partitionResult The validation results for the given partition
         * @param partitionId Partition to validate
         * @return True if no error in validation. False otherwise
         */
        private boolean verifyValidation(ValidationResult.ValidationType type,
                                         PartitionValidationResults partitionResult,
                                         int partitionId) {

            ValidationResult validationResult = partitionResult.validationResultsMap.get(type);
            if (validationResult.status.equals(ValidationResult.Status.FAILURE)) {
                outputBuilder.append(String.format("Validation %s failed for partition %s%n", type.name(), partitionId));
                outputBuilder.append(String.format("  Validation error is: %s%n", validationResult.error.replace("\n", "\n  ")));
                return false;
            }
            return true;
        }

        /**
         * Builds {@code SERVER_STORAGE_CONNECTIVITY} {@code PartitionValidationResults} for all the servers in the
         * cluster.
         *
         * @param rpcClient Client used to connect to Waltz server to fetch connectivity statuses.
         * @param clusterManager ClusterManager used to fetch cluster related information from Zookeeper.
         * @param replicaAssignments Replica assignments for all the partitions.
         * @param partitionValidationResultsList a {@code List<PartitionValidationResults>} that will contain all the
         *                                       validation when the returned CompletableFuture is complete.
         * @return a {@code CompletableFuture<Void>} which will complete after all the
         * {@code PartitionValidationResults} are available.
         * @throws InterruptedException if thrown by the {@code InternalRpcClient}
         * @throws ClusterManagerException if thrown by the {@code ClusterManager}
         */
        private CompletableFuture<Void> buildServerStorageConnectivityValidation(
            InternalRpcClient rpcClient,
            ClusterManager clusterManager,
            ReplicaAssignments replicaAssignments,
            List<PartitionValidationResults> partitionValidationResultsList
        ) throws InterruptedException, ClusterManagerException {

            PartitionAssignment partitionAssignment = clusterManager.partitionAssignment();
            Map<Endpoint, List<PartitionInfo>> partitionAssignments =
                clusterManager.serverDescriptors()
                    .stream()
                    .collect(
                        Collectors.toMap(
                            descriptor -> descriptor.endpoint,
                            descriptor -> partitionAssignment.partitionsFor(descriptor.serverId),
                            (oldKey, newKey) -> newKey
                        )
                    );

            CompletableFuture<Map<Endpoint, Map<String, Boolean>>> connectivityStatusFuture =
                (CompletableFuture<Map<Endpoint, Map<String, Boolean>>>)
                    rpcClient.checkServerConnections(partitionAssignments.keySet());

            return connectivityStatusFuture.thenAccept(response -> {
                    Map<Integer, Set<String>> partitionIdToReplicas = new HashMap<>();
                    replicaAssignments
                        .replicas
                        .forEach((replica, assignedPartitions) ->
                            Arrays
                                .stream(assignedPartitions)
                                .forEach(partitionId -> {
                                    partitionIdToReplicas.putIfAbsent(partitionId, new HashSet<>());
                                    partitionIdToReplicas.get(partitionId).add(replica);
                                })
                        );

                    partitionAssignments.forEach((endpoint, partitions) -> {
                        Map<String, Boolean> storageConnectivityResults = response.get(endpoint);
                        partitions.forEach(partition ->
                            partitionValidationResultsList
                                .get(partition.partitionId)
                                .validationResultsMap
                                .put(
                                    ValidationResult.ValidationType.SERVER_STORAGE_CONNECTIVITY,
                                    buildStorageConnectivityValidationResult(
                                        partitionIdToReplicas.get(partition.partitionId),
                                        storageConnectivityResults,
                                        String.format("Server %s", endpoint)
                                    )
                                )
                        );
                    });
                });
        }

        private ValidationResult buildStorageConnectivityValidationResult(
            Set<String> storages,
            Map<String, Boolean> storageConnectivityResults,
            String errMsgPrefix
        ) {
            ValidationResult.Status validationStatus = ValidationResult.Status.SUCCESS;
            String errorMsg = "";

            if (Objects.isNull(storageConnectivityResults)) {
                validationStatus = ValidationResult.Status.FAILURE;
                errorMsg = String.format("%s, no connectivity statuses response", errMsgPrefix);
            } else {
                for (String storage : storages) {
                    if (!storageConnectivityResults.containsKey(storage)) {
                        validationStatus = ValidationResult.Status.FAILURE;
                        errorMsg = errorMsg.concat(System.lineSeparator()).concat(
                            String.format(
                                "%s, storage replica %s missing in connectivity statuses",
                                errMsgPrefix, storage
                            )
                        );
                    } else if (!storageConnectivityResults.get(storage)) {
                        validationStatus = ValidationResult.Status.FAILURE;
                        errorMsg = errorMsg.concat(System.lineSeparator()).concat(
                            String.format(
                                "%s, storage connectivity check for storage replica %s failed",
                                errMsgPrefix, storage
                            )
                        );
                    }
                }
            }

            return new ValidationResult(
                ValidationResult.ValidationType.SERVER_STORAGE_CONNECTIVITY,
                validationStatus,
                errorMsg
            );
        }

        /**
         * Validate for a single server in the cluster the consistency of partition assignments on the actual server
         * versus on Zookeeper metadata.
         *
         * @param rpcClient Client used to connect to Waltz server to fetch partition assignments
         * @param serverDescriptor Actual server to run the validation for
         * @param clusterManager Contains zkclient used to fetch partition assignments from Zookeeper
         * @param partitionValidationResultsList List indexed by partition in which validation outputs are inserted
         * @return Completable future that complete once the partitionValidationResultsList is filled with validation data
         *          from all partitions from that server
         * @throws InterruptedException If thread interrupted while waiting for channel with Waltz server to be ready
         * @throws ClusterManagerException Thrown if Zookeeper is missing some ZNodes or ZNode values
         */
        private CompletableFuture<Void> buildServerZKPartitionAssignmentsValidation(InternalRpcClient rpcClient,
                                                                                    ServerDescriptor serverDescriptor,
                                                                                    ClusterManager clusterManager,
                                                                                    List<PartitionValidationResults> partitionValidationResultsList)
                throws InterruptedException, ClusterManagerException {
            CompletableFuture<List<Integer>> futureResponse =
                (CompletableFuture<List<Integer>>) rpcClient.getServerPartitionAssignments(serverDescriptor.endpoint);
            List<PartitionInfo> zookeeperAssignments =
                clusterManager.partitionAssignment().partitionsFor(serverDescriptor.serverId);
            return futureResponse
                    .thenAccept(serverAssignments -> {
                        Map<Integer, AssignmentMatch> assignmentMatchMap = verifyAssignments(zookeeperAssignments, serverAssignments);

                        assignmentMatchMap.forEach((partitionId, match) -> {
                            ValidationResult.Status status = ValidationResult.Status.SUCCESS;
                            String error = "";
                            PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionId);
                            ValidationResult zkValidationResult =
                                partitionValidationResults.validationResultsMap.get(ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_VALIDITY);

                            if (zkValidationResult.status == ValidationResult.Status.FAILURE) {
                                status = ValidationResult.Status.FAILURE;
                                error = zkValidationResult.error;
                            } else if (!match.equals(AssignmentMatch.IN_BOTH)) {
                                status = ValidationResult.Status.FAILURE;
                                error = "Partition " + partitionId + " not matching in zk and server: " + match.name();
                            }

                            ValidationResult validationResult = new ValidationResult(ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                                                                        status, error);
                            partitionValidationResults.validationResultsMap.put(validationResult.type,
                                validationResult);
                        });
                    }).exceptionally(e -> {
                        for (PartitionInfo partitionInfo : zookeeperAssignments) {
                            PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionInfo.partitionId);
                            ValidationResult validationResult = new ValidationResult(
                                    ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                                    ValidationResult.Status.FAILURE,
                                    e.getMessage());
                            partitionValidationResults.validationResultsMap.put(validationResult.type,
                                validationResult);
                        }
                        return null;
                    });
        }

        /**
         *
         * Validate for all servers in the cluster the consistency of partition assignments on the actual servers
         * versus on Zookeeper metadata.
         *
         * @param rpcClient Client used to connect to Waltz servers to fetch partition assignments
         * @param clusterManager Contains zkclient used to fetch partition assignments from Zookeeper
         * @param partitionValidationResultsList List indexed by partition in which validation outputs are inserted
         * @return Completable future that complete once the partitionValidationResultsList is filled with validation data
         * from all partitions
         * @throws ClusterManagerException Thrown if Zookeeper is missing some ZNodes or ZNode values
         * @throws InterruptedException If thread interrupted while waiting for channel with Waltz servers to be ready
         */
        private CompletableFuture<Void> buildServersZKPartitionAssignmentsConsistencyValidation(InternalRpcClient rpcClient,
                                                                                                ClusterManager clusterManager,
                                                                                                List<PartitionValidationResults> partitionValidationResultsList)
                                                                    throws ClusterManagerException, InterruptedException {
            Set<CompletableFuture> futures = new HashSet<>();

            for (ServerDescriptor serverDescriptor : clusterManager.serverDescriptors()) {
                CompletableFuture<Void> future = buildServerZKPartitionAssignmentsValidation(rpcClient, serverDescriptor, clusterManager,
                         partitionValidationResultsList);
                futures.add(future);
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        }

        /**
         *
         * Validate for all servers in the cluster the validity of partition
         * generation on the actual servers versus on Zookeeper store replica metadata.
         * Partition generation on a server owning a partition should be >= partition generation stored on ZooKeeper under store replica
         *
         * @param rpcClient Client used to connect to Waltz servers to fetch partition information
         * @param clusterManager Contains zkclient used to fetch partition information from Zookeeper
         * @param partitionRoot ZNode used to fetch replica states for each partition.
         * @param zkClient ZooKeeper Client used to fetch partition metadata.
         * @param partitionGenerationValidationResultsList List indexed by partition in which validation outputs are inserted
         * @return Completable future that complete once the partitionGenerationValidationResultsList is filled with validation data
         * from all partitions
         * @throws ClusterManagerException Thrown if Zookeeper is missing some ZNodes or ZNode values
         * @throws InterruptedException If thread interrupted while waiting for channel with Waltz servers to be ready
         */
        private CompletableFuture<Void> buildServersZKPartitionMetadataCorrectnessValidation(InternalRpcClient rpcClient,
                                                                                             ClusterManager clusterManager,
                                                                                             ZNode partitionRoot,
                                                                                             ZooKeeperClient zkClient,
                                                                                             List<PartitionValidationResults> partitionGenerationValidationResultsList)
                throws ClusterManagerException, InterruptedException {
            Set<CompletableFuture> futures = new HashSet<>();

            for (ServerDescriptor serverDescriptor : clusterManager.serverDescriptors()) {
                List<PartitionInfo> zookeeperAssignments =
                    clusterManager.partitionAssignment().partitionsFor(serverDescriptor.serverId);
                CompletableFuture<Void> future = buildServersZKPartitionMetadataValidation(rpcClient, serverDescriptor,
                    partitionRoot, zkClient, partitionGenerationValidationResultsList).exceptionally(e -> {
                    for (PartitionInfo partitionInfo : zookeeperAssignments) {
                        PartitionValidationResults partitionValidationResults = partitionGenerationValidationResultsList.get(partitionInfo.partitionId);
                        ValidationResult validationResult = new ValidationResult(
                            ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                            ValidationResult.Status.FAILURE,
                            e.getMessage());
                        partitionValidationResults.validationResultsMap.put(validationResult.type,
                            validationResult);
                    }
                    return null;
                });
                futures.add(future);
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        }

        /**
         * Validate for a single server in the cluster that all partitions belonging to this server have valid generation
         * number counterpart stored on Zookeeper replica node. This means generation value metadata on Zookeeper replica
         * node cannot be higher than partition generation value stored on server.
         *
         * @param rpcClient Client used to connect to Waltz server to fetch partition assignments
         * @param serverDescriptor Actual server to run the validation for
         * @param partitionRoot partitionRoot ZNode used to fetch replica states for each partition.
         * @param zkClient ZooKeeper Client used to fetch partition metadata.
         * @param partitionGenerationValidationResultsList List indexed by partition in which validation outputs are inserted
         * @return Completable future that complete once the partitionGenerationValidationResultsList is filled with validation data
         *          from all partitions from that server
         * @throws InterruptedException If thread interrupted while waiting for channel with Waltz server to be ready
         * @throws ClusterManagerException Thrown if Zookeeper is missing some ZNodes or ZNode values
         */
        private CompletableFuture<Void> buildServersZKPartitionMetadataValidation(InternalRpcClient rpcClient,
                                                                                  ServerDescriptor serverDescriptor,
                                                                                  ZNode partitionRoot,
                                                                                  ZooKeeperClient zkClient,
                                                                                  List<PartitionValidationResults> partitionGenerationValidationResultsList)
                throws InterruptedException {
            CompletableFuture<List<PartitionInfo>> futureResponse =
                (CompletableFuture<List<PartitionInfo>>) rpcClient.getServerPartitionInfo(serverDescriptor.endpoint);

            return futureResponse
                .thenAccept(serverPartitionInfo -> {
                    serverPartitionInfo.forEach(partitionInfo -> {
                        ValidationResult.Status status = ValidationResult.Status.SUCCESS;
                        String error = "";
                        PartitionValidationResults partitionValidationResults = partitionGenerationValidationResultsList.get(partitionInfo.partitionId);

                        ZNode zNode = new ZNode(partitionRoot, Integer.toString(partitionInfo.partitionId));
                        try {
                            PartitionMetadata partitionMetadata = zkClient.getData(zNode, PartitionMetadataSerializer.INSTANCE).value;
                            if (partitionMetadata.generation > partitionInfo.generation) {
                                status = ValidationResult.Status.FAILURE;
                                error = String.format("Store generation in ZooKeeper is ahead of server. Server partition generation: %s, "
                                    + "Zookeeper partition generation: %s", partitionInfo.generation, partitionMetadata.generation);
                            }
                        } catch (KeeperException | ZooKeeperClientException e) {
                            status = ValidationResult.Status.FAILURE;
                            error = String.format("Cannot obtain replica states, exception caught: %s", e);
                        }

                        ValidationResult validationResult = new ValidationResult(ValidationResult.ValidationType.PARTITION_GENERATION_ZK_SERVER_CONSISTENCY,
                            status, error);
                        partitionValidationResults.validationResultsMap.put(validationResult.type,
                            validationResult);
                    });
                });
            }

        /**
         * Status of the partition assignment for a server
         */
        enum AssignmentMatch {
            ONLY_IN_ZOOKEEPER,
            ONLY_IN_SERVER,
            IN_BOTH
        }

        /**
         * Compares waltz server partition assignment lists on Zookeeper metadata versus on actual servers
         *
         * @param zookeeperAssignments List of server partition assignment metadata on Zookeeper
         * @param serverAssignments List of partition assignment on actual server
         * @return Map containing the comparison result for each partition
         */
        private Map<Integer, AssignmentMatch> verifyAssignments(List<PartitionInfo> zookeeperAssignments,
                                       List<Integer> serverAssignments) {

            Map<Integer, AssignmentMatch> assignmentMatchHashMap = new HashMap<>();
            for (PartitionInfo partitionInfo: zookeeperAssignments) {
                assignmentMatchHashMap.put(partitionInfo.partitionId, AssignmentMatch.ONLY_IN_ZOOKEEPER);
            }

            for (Integer partitionId : serverAssignments) {
                if (assignmentMatchHashMap.containsKey(partitionId)) {
                    assignmentMatchHashMap.put(partitionId, AssignmentMatch.IN_BOTH);
                } else {
                    assignmentMatchHashMap.put(partitionId, AssignmentMatch.ONLY_IN_SERVER);
                }
            }
            return assignmentMatchHashMap;
        }

        /**
         * Parses the zookeeper servers partition assignments ZNode and insert the validation result into
         * the partitionValidationResultsList for each partition
         *
         * @param clusterManager Contains zkclient used to fetch partition assignments from Zookeeper
         * @param partitionValidationResultsList List indexed by partition in which validation outputs are inserted
         * @throws ClusterManagerException Thrown if Zookeeper is missing some ZNodes or ZNode values
         */
        private void buildZookeeperPartitionAssignmentsValidation(ClusterManager clusterManager,
                                                                  List<PartitionValidationResults> partitionValidationResultsList) throws ClusterManagerException {
            PartitionAssignment partitionAssignment = clusterManager.partitionAssignment();
            int[] partitionToServerMap = new int[clusterManager.numPartitions()];

            for (int serverId : partitionAssignment.serverIds()) {
                for (PartitionInfo partitionInfo : partitionAssignment.partitionsFor(serverId)) {
                    String error = "";

                    if (partitionInfo.partitionId < 0 || partitionInfo.partitionId >= clusterManager.numPartitions()) {
                        error = "Error: Server " + serverId + " handles invalid partition " + partitionInfo.partitionId;
                    } else if (partitionToServerMap[partitionInfo.partitionId] != 0) {
                        error = "Error: Partition handled by more than one server: "
                                + partitionToServerMap[partitionInfo.partitionId] + " and " + serverId;
                    } else {
                        partitionToServerMap[partitionInfo.partitionId] = serverId;
                    }

                    ValidationResult validationResult = new ValidationResult(
                                ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_VALIDITY,
                                ("".equals(error)) ? ValidationResult.Status.SUCCESS : ValidationResult.Status.FAILURE,
                                error);
                    PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionInfo.partitionId);
                    partitionValidationResults.validationResultsMap.put(validationResult.type, validationResult);
                }
            }

            for (int partitionId = 0; partitionId < clusterManager.numPartitions(); partitionId++) {
                if (partitionToServerMap[partitionId] == 0) {
                    ValidationResult validationResult = new ValidationResult(
                            ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_VALIDITY,
                            ValidationResult.Status.FAILURE,
                            "Error: Partition " + partitionId + " not handled by any server");
                    PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionId);
                    partitionValidationResults.validationResultsMap.put(validationResult.type, validationResult);
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }

        /**
         * Class to contain all types of validations made to a specific partition
         */
        private static class PartitionValidationResults {
            private int partitionId;

            private Map<ValidationResult.ValidationType, ValidationResult> validationResultsMap;

            PartitionValidationResults(int partitionId) {
                this.partitionId = partitionId;
                this.validationResultsMap =
                    Collections.synchronizedMap(new EnumMap<>(ValidationResult.ValidationType.class));
            }

            @Override
            public String toString() {
               return "partitionId " + partitionId + " \n " + " map "
                       + Arrays.toString(validationResultsMap.values().toArray());
            }
        }

        /**
         * Class to contain actual validation data for a specific validation type
         */
        private static class ValidationResult {
            enum ValidationType {
                PARTITION_ASSIGNMENT_ZK_VALIDITY,
                PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                SERVER_STORAGE_CONNECTIVITY,
                PARTITION_ASSIGNMENT_ZK_STORAGE_CONSISTENCY,
                PARTITION_QUORUM_STATUS,
                PARTITION_GENERATION_ZK_SERVER_CONSISTENCY,
                REPLICA_RECOVERY_STATUS
            }

            enum Status {
                SUCCESS,
                FAILURE
            }

            private ValidationType type;
            private Status status;
            private String error;

            ValidationResult(ValidationType type, Status status, String error) {
                this.type = type;
                this.status = status;
                this.error = error;
            }

            @Override
            public String toString() {
                return "type: " + type + " status: " + status + " error: " + error;
            }
        }

    }

    /**
     * A transaction callback to help construct {@link WaltzClient}. It is dummy because
     * it is not suppose to receive any callbacks.
     */
    private static final class DummyTxnCallbacks implements WaltzClientCallbacks {

        @Override
        public long getClientHighWaterMark(int partitionId) {
            return -1L;
        }

        @Override
        public void applyTransaction(Transaction transaction) {
        }

        @Override
        public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
        }
    }

    public static void testMain(String[] args) {
        new ClusterCli(args, true).processCmd();
    }

    public static void main(String[] args) {
        new ClusterCli(args, false).processCmd();
    }
}
