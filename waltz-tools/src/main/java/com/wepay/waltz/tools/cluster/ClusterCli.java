package com.wepay.waltz.tools.cluster;

import com.wepay.riff.network.ClientSSL;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.InternalRpcClient;
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
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;

/**
 * ClusterCli is a tool for interacting with the Waltz Cluster.
 */
public final class ClusterCli extends SubcommandCli {

    private ClusterCli(String[] args,  boolean useByTest) {
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
            try {
                String cliConfigPath = cmd.getOptionValue("cli-config-path");
                CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
                String zookeeperHostPorts = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
                String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
                int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout);
                ZNode root = new ZNode(zkRoot);

                Set<Endpoint> serverEndpoints = getListOfServerEndpoints(root, zkClient);
                WaltzClientConfig waltzClientConfig = getWaltzClientConfig(cliConfigPath);
                Map<Endpoint, Map<String, Boolean>> connectivityStatusMap =
                    checkServerConnections(waltzClientConfig,
                        serverEndpoints);

                for (Map.Entry<Endpoint, Map<String, Boolean>> connectivityStatus : connectivityStatusMap.entrySet()) {
                    System.out.println("Connectivity status of " + connectivityStatus.getKey() + " is: "
                        + ((connectivityStatus.getValue() == null) ? "UNREACHABLE"
                        : connectivityStatus.getValue().toString()));
                }
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to check topology of the cluster. %n%s",
                    e.getMessage()));
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        private Set<Endpoint> getListOfServerEndpoints(ZNode root, ZooKeeperClient zkClient) throws Exception {
            ClusterManager clusterManager = new ClusterManagerImpl(zkClient, root, partitionAssignmentPolicy);
            Set<ServerDescriptor> serverDescriptors = clusterManager.serverDescriptors();
            Set<Endpoint> serverEndpoints = new HashSet<>();

            for (ServerDescriptor serverDescriptor: serverDescriptors) {
                serverEndpoints.add(serverDescriptor.endpoint);
            }
            return serverEndpoints;
        }

        private Map<Endpoint, Map<String, Boolean>> checkServerConnections(WaltzClientConfig config,
                                                                           Set<Endpoint> serverEndpoints) throws Exception {
            DummyTxnCallbacks callbacks = new DummyTxnCallbacks();
            InternalRpcClient rpcClient = new InternalRpcClient(ClientSSL.createContext(config.getSSLConfig()),
                WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, callbacks);

            return rpcClient.checkServerConnections(serverEndpoints).get();
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
     * ....
     */
    private static final class Verify extends Cli {
        private static final String NAME = "verify";
        private static final String DESCRIPTION = "Validates if partition(s) is handled by some server";

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
                String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
                int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout);

                ClusterManager clusterManager = new ClusterManagerImpl(zkClient, new ZNode(zkRoot), partitionAssignmentPolicy);

                for (int partitionId = 0; partitionId < clusterManager.numPartitions(); partitionId++) {
                    partitionsValidationResultList.add(new PartitionValidationResults(partitionId));
                }

                // Step1: validate all partitions on zk
                buildZookeeperPartitionAssignmentsValidation(zkClient, zkRoot, partitionsValidationResultList);
                verifyValidation(ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_VALIDITY, partitionsValidationResultList);

                // Step2: validate zk and servers partition assignment consistency
                rpcClient = new InternalRpcClient(ClientSSL.createContext(waltzClientConfig.getSSLConfig()),
                        WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, new DummyTxnCallbacks());

                buildServersZKPartitionAssignmentsConsistencyValidation(rpcClient, clusterManager, partitionsValidationResultList).get();

                for (PartitionValidationResults results : partitionsValidationResultList) {
                    System.out.println(results);
                    if (!results.validationResultsMap.containsKey(ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY)) {
                        ValidationResults validationResults = new ValidationResults(ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                                ValidationResults.Status.FAILURE, "Timeout exception");
                        results.validationResultsMap.put(ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY, validationResults);
                    }
                }
                verifyValidation(ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY, partitionsValidationResultList);

            } catch (Exception e) {
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

        /**
         *
         *  Verify a validation type for all partitions from the list of validation results.
         *  If validation failed, errors are printed
         *
         * @param type Type of validation
         * @param results List containing the validation results for each partition
         * @return True if no error in validation. False otherwise
         */
        private boolean verifyValidation(ValidationResults.ValidationType type, List<PartitionValidationResults> results) {
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
        private boolean verifyValidation(ValidationResults.ValidationType type, List<PartitionValidationResults> results, int partitionId) {
            PartitionValidationResults partitionResult = results.get(partitionId);

            ValidationResults partitionZkResults = partitionResult.validationResultsMap.get(type);
            if (partitionZkResults.status.equals(ValidationResults.Status.FAILURE)) {
                System.out.println("Validation " + type.name() + " failed for partition " + partitionId);
                System.out.println("Error is: " + partitionZkResults.error);
                return false;
            }
            return true;
        }

        /**
         * Validate for a single server in the cluster the consistency of partition assignments on the actual server
         * versus on Zookeeper metadata.
         *
         * @param rpcClient Client used to connect to Waltz server to fetch partition assignments
         * @param server Actual server to run the validation for
         * @param clusterManager Contains zkclient used to fetch partition assignments of Zookeeper
         * @param partitionValidationResultsList List indexed by partition in which validation outputs are inserted
         * @return Completable future that complete once the partitionValidationResultsList is filled with validation data
         *          from all partitions from that server
         * @throws InterruptedException If thread interrupted while waiting for channel with Waltz server to be ready
         * @throws ClusterManagerException Thrown if Zookeeper is missing some ZNodes or ZNode values
         */
        private CompletableFuture<Void> buildServerZKPartitionAssignmentsValidation(InternalRpcClient rpcClient,
                                                                                    ServerDescriptor server,
                                                                                    ClusterManager clusterManager,
                                                                                    List<PartitionValidationResults> partitionValidationResultsList)
                throws InterruptedException, ClusterManagerException {
            CompletableFuture<Object> futureResponse = (CompletableFuture<Object>) rpcClient.getServerPartitionAssignments(server.endpoint);
            List<PartitionInfo> zookeeperAssignments = clusterManager.partitionAssignment().partitionsFor(server.serverId);
            return futureResponse
                    .thenAccept(v -> {
                        List<Integer> serverAssignments = (List<Integer>) v;
                        Map<Integer, AssignmentMatch> assignmentMatchMap = verifyAssignments(zookeeperAssignments, serverAssignments);

                        assignmentMatchMap.forEach((partitionId, match) -> {
                            ValidationResults.Status status = ValidationResults.Status.SUCCESS;
                            String error = "";

                            if (!match.equals(AssignmentMatch.IN_BOTH)) {
                                status = ValidationResults.Status.FAILURE;
                                error = "Partition " + partitionId + " not matching in zk and server: " + match.name();
                            }

                            PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionId);
                            ValidationResults validationResults = new ValidationResults(ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                                                                        status, error);
                            partitionValidationResults.validationResultsMap.put(validationResults.type, validationResults);
                        });
                    }).exceptionally(e -> {
                        for (PartitionInfo partitionInfo : zookeeperAssignments) {
                            PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionInfo.partitionId);
                            ValidationResults validationResults = new ValidationResults(
                                    ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY,
                                    ValidationResults.Status.FAILURE,
                                    e.getMessage());
                            partitionValidationResults.validationResultsMap.put(validationResults.type, validationResults);
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
         * @param clusterManager Contains zkclient used to fetch partition assignments of Zookeeper
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
            Set<CompletableFuture> set = new HashSet<>();

            for (ServerDescriptor server : clusterManager.serverDescriptors()) {
                CompletableFuture<Void> future = buildServerZKPartitionAssignmentsValidation(rpcClient, server, clusterManager,
                         partitionValidationResultsList);
                set.add(future);
            }
            return CompletableFuture.allOf(set.toArray(new CompletableFuture[set.size()]));
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
         * @param zkClient Zookeeper client used to fetch Zookeeper partition assignment from
         * @param zkRoot Zookeeper root path for cluster
         * @param partitionValidationResultsList List indexed by partition in which validation outputs are inserted
         * @throws ClusterManagerException Thrown if Zookeeper is missing some ZNodes or ZNode values
         */
        private void buildZookeeperPartitionAssignmentsValidation(ZooKeeperClient zkClient,
                                                                  String zkRoot,
                                                                  List<PartitionValidationResults> partitionValidationResultsList) throws ClusterManagerException {

            ClusterManager clusterManager = new ClusterManagerImpl(zkClient, new ZNode(zkRoot), partitionAssignmentPolicy);
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

                    ValidationResults validationResults = new ValidationResults(
                                ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_VALIDITY,
                                ("".equals(error)) ? ValidationResults.Status.SUCCESS : ValidationResults.Status.FAILURE,
                                error);
                    PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionInfo.partitionId);
                    partitionValidationResults.validationResultsMap.put(validationResults.type, validationResults);
                }
            }

            for (int partitionId = 0; partitionId < clusterManager.numPartitions(); partitionId++) {
                if (partitionToServerMap[partitionId] == 0) {
                    ValidationResults validationResults = new ValidationResults(
                            ValidationResults.ValidationType.PARTITION_ASSIGNMENT_ZK_VALIDITY,
                            ValidationResults.Status.FAILURE,
                            "Error: Partition " + partitionId + " not handled by any server");
                    PartitionValidationResults partitionValidationResults = partitionValidationResultsList.get(partitionId);
                    partitionValidationResults.validationResultsMap.put(validationResults.type, validationResults);
                    partitionValidationResultsList.add(partitionValidationResults);
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

            private Map<Verify.ValidationResults.ValidationType, ValidationResults> validationResultsMap;

            PartitionValidationResults(int partitionId) {
                this.partitionId = partitionId;
                this.validationResultsMap = new EnumMap<>(ValidationResults.ValidationType.class);
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
        private static class ValidationResults {
            enum ValidationType {
                PARTITION_ASSIGNMENT_ZK_VALIDITY,
                PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY
            }

            enum Status {
                SUCCESS,
                FAILURE
            }

            private ValidationType type;
            private Status status;
            private String error;

            ValidationResults(ValidationType type, Status status, String error) {
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
