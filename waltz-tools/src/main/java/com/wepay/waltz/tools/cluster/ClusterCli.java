package com.wepay.waltz.tools.cluster;

import com.wepay.riff.network.ClientSSL;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.InternalRpcClient;
import com.wepay.waltz.common.metadata.ReplicaAssignments;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.exception.SubCommandFailedException;
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
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
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

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout);
                ZNode root = new ZNode(zkRoot);

                ClusterManager clusterManager = new ClusterManagerImpl(zkClient, root, partitionAssignmentPolicy);
                Set<Endpoint> serverEndpoints =
                    clusterManager.serverDescriptors()
                        .stream()
                        .map(serverDescriptor -> serverDescriptor.endpoint)
                        .collect(Collectors.toSet());

                WaltzClientConfig waltzClientConfig = getWaltzClientConfig(cliConfigPath);

                DummyTxnCallbacks callbacks = new DummyTxnCallbacks();
                rpcClient = new InternalRpcClient(ClientSSL.createContext(waltzClientConfig.getSSLConfig()),
                    WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, callbacks);

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
     * ...
     */
    private static final class Verify extends Cli {
        private static final String NAME = "verify";
        private static final String DESCRIPTION = "Validates if partition(s) is handled by some server";
        private static final long TIMEOUT_IN_SECONDS = 5;

        private final PartitionAssignmentPolicy partitionAssignmentPolicy = new DynamicPartitionAssignmentPolicy();

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

            cliCfgOption.setRequired(true);
            partitionOption.setRequired(false);

            options.addOption(cliCfgOption);
            options.addOption(partitionOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            ZooKeeperClient zkClient = null;
            InternalRpcClient rpcClient = null;
            List<PartitionValidationResults> partitionsValidationResultList = new ArrayList<>();
            try {
                String cliConfigPath = cmd.getOptionValue("cli-config-path");
                WaltzClientConfig waltzClientConfig = getWaltzClientConfig(cliConfigPath);
                CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
                String zookeeperHostPorts = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
                ZNode zkRoot = new ZNode((String) cliConfig.get(CliConfig.CLUSTER_ROOT));
                ZNode storeRoot = new ZNode(zkRoot, StoreMetadata.STORE_ZNODE_NAME);
                int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout);

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, storeRoot);

                ClusterManager clusterManager = new ClusterManagerImpl(zkClient, zkRoot, partitionAssignmentPolicy);

                for (int partitionId = 0; partitionId < clusterManager.numPartitions(); partitionId++) {
                    partitionsValidationResultList.add(new PartitionValidationResults(partitionId));
                }

                // Step1: Validate all partitions on zk
                buildZookeeperPartitionAssignmentsValidation(clusterManager, partitionsValidationResultList);
                verifyValidation(
                    ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_VALIDITY, partitionsValidationResultList
                );

                // Step2: Build zk and servers partition assignment consistency validations
                rpcClient = new InternalRpcClient(ClientSSL.createContext(waltzClientConfig.getSSLConfig()),
                        WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, new DummyTxnCallbacks());

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

                // Verify validations for Step2 and Step3
                try {
                    consistencyValidationFuture.get(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException exception) {
                    failMissingValidationResults(
                        partitionsValidationResultList,
                        ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                        exception.toString()
                    );
                }

                try {
                    connectivityValidationFuture.get(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException exception) {
                    failMissingValidationResults(
                        partitionsValidationResultList,
                        ValidationResult.ValidationType.SERVER_STORAGE_CONNECTIVITY,
                        exception.toString()
                    );
                }

                verifyValidation(
                    ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                    partitionsValidationResultList
                );

                verifyValidation(
                    ValidationResult.ValidationType.SERVER_STORAGE_CONNECTIVITY,
                    partitionsValidationResultList
                );
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
            }
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

        /**
         *  Verify a validation type for all partitions from the list of validation results.
         *  If validation failed, errors are printed
         *
         * @param type Type of validation
         * @param results List containing the validation results for each partition
         * @return True if no error in validation. False otherwise
         */
        private boolean verifyValidation(ValidationResult.ValidationType type,
                                         List<PartitionValidationResults> results) {
            boolean success = true;
            for (int partitionId = 0; partitionId < results.size(); partitionId++) {
                if (!verifyValidation(type, results, partitionId)) {
                    success = false;
                }
            }
            return success;
        }

        /**
         * Verify a validation type for a specific partition from the list of validation results.
         * If validation failed, error is printed
         *
         * @param type Type of validation
         * @param results List containing the validation results for each partition
         * @param partitionId Partition to validate
         * @return True if no error in validation. False otherwise
         */
        private boolean verifyValidation(ValidationResult.ValidationType type,
                                         List<PartitionValidationResults> results,
                                         int partitionId) {
            PartitionValidationResults partitionResult = results.get(partitionId);

            ValidationResult partitionZkResults = partitionResult.validationResultsMap.get(type);
            if (partitionZkResults.status.equals(ValidationResult.Status.FAILURE)) {
                System.out.println("Validation " + type.name() + " failed for partition " + partitionId);
                System.out.println("Validation error is: " + partitionZkResults.error);
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
            CompletableFuture<List<Integer>> futureResponse = (CompletableFuture<List<Integer>>) rpcClient.getServerPartitionAssignments(serverDescriptor.endpoint);
            List<PartitionInfo> zookeeperAssignments = clusterManager.partitionAssignment().partitionsFor(serverDescriptor.serverId);
            return futureResponse
                    .thenAccept(serverAssignments -> {
                        Map<Integer, AssignmentMatch> assignmentMatchMap = verifyAssignments(zookeeperAssignments, serverAssignments);

                        assignmentMatchMap.forEach((partitionId, match) -> {
                            ValidationResult.Status status = ValidationResult.Status.SUCCESS;
                            String error = "";

                            if (!match.equals(AssignmentMatch.IN_BOTH)) {
                                status = ValidationResult.Status.FAILURE;
                                error = "Partition " + partitionId + " not matching in zk and server: " + match.name();
                            }

                            PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionId);
                            ValidationResult validationResult = new ValidationResult(ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                                                                        status, error);
                            partitionValidationResults.validationResultsMap.put(validationResult.type, validationResult);
                        });
                    }).exceptionally(e -> {
                        for (PartitionInfo partitionInfo : zookeeperAssignments) {
                            PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionInfo.partitionId);
                            ValidationResult validationResult = new ValidationResult(
                                    ValidationResult.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                                    ValidationResult.Status.FAILURE,
                                    e.getMessage());
                            partitionValidationResults.validationResultsMap.put(validationResult.type, validationResult);
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
                this.validationResultsMap = new EnumMap<>(ValidationResult.ValidationType.class);
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
                SERVER_STORAGE_CONNECTIVITY
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
     * Return an object of {@code WaltzClientConfig} built from configuration file.
     * @param configFilePath the path to configuration file
     * @return WaltzClientConfig
     * @throws IOException
     */
    private static WaltzClientConfig getWaltzClientConfig(String configFilePath) throws IOException {
        Yaml yaml = new Yaml();
        try (FileInputStream in = new FileInputStream(configFilePath)) {
            Map<Object, Object> props = yaml.load(in);
            props.put(WaltzClientConfig.AUTO_MOUNT, false);
            return new WaltzClientConfig(props);
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
