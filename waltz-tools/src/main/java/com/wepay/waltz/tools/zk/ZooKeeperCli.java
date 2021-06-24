package com.wepay.waltz.tools.zk;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.common.metadata.ConnectionMetadata;
import com.wepay.waltz.common.metadata.GroupDescriptor;
import com.wepay.waltz.common.metadata.PartitionMetadata;
import com.wepay.waltz.common.metadata.PartitionMetadataSerializer;
import com.wepay.waltz.common.metadata.ReplicaAssignments;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.common.metadata.ReplicaState;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.metadata.StoreParams;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.waltz.tools.CliUtils;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.ClusterParams;
import com.wepay.zktools.clustermgr.internal.ClusterParamsSerializer;
import com.wepay.zktools.clustermgr.internal.DynamicPartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.tools.CreateCluster;
import com.wepay.zktools.clustermgr.tools.ListCluster;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * ZooKeeperCli is a tool for interacting with Waltz ZooKeeper data.
 */
public final class ZooKeeperCli extends SubcommandCli {

    private ZooKeeperCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
                new Subcommand(Create.NAME, Create.DESCRIPTION, Create::new),
                new Subcommand(Delete.NAME, Delete.DESCRIPTION, Delete::new),
                new Subcommand(ListZk.NAME, ListZk.DESCRIPTION, ListZk::new),
                new Subcommand(ShowClusterKey.NAME, ShowClusterKey.DESCRIPTION, ShowClusterKey::new),
                new Subcommand(Add.NAME, Add.DESCRIPTION, Add::new),
                new Subcommand(Remove.NAME, Remove.DESCRIPTION, Remove::new),
                new Subcommand(Assign.NAME, Assign.DESCRIPTION, Assign::new),
                new Subcommand(Unassign.NAME, Unassign.DESCRIPTION, Unassign::new),
                new Subcommand(AutoAssign.NAME, AutoAssign.DESCRIPTION, AutoAssign::new)
        ));
    }

    /**
     * The {@code list} command dumps ZooKeeper metadata to the console. The metadata
     * shows which Waltz servers are assigned to which partitions. Example output:
     *
     * <pre>
     * cluster root [/demo]:
     *   name=demo cluster
     *   numPartitions=1
     * cluster root [/demo] has server descriptors:
     *   server=1, endpoint=chrisr-01PHHF1T:8000, preferred partitions=[*]
     * cluster root [/demo] has partition assignment metadata:
     *   cversion=1, endpoints=1, partitions=1
     * cluster root [/demo] has partition assignments:
     *   server=1, partition=0, generation=1
     * store [/demo/store] parameters:
     *   key=339476fd-c80f-4ace-a26a-66dcbe6af680
     *   numPartitions=1
     *   numReplicas=1
     * store [/demo/store] replica assignments:
     *   replicas=1
     *     localhost:6000 = [0]
     * </pre>
     */
    private static final class ListZk extends Cli {
        private static final String NAME = "list";
        private static final String DESCRIPTION = "Displays server metadata.";
        private static final StringBuilder OUTPUT_BUILDER = new StringBuilder();

        protected ListZk(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option cliCfgOption = Option.builder("c")
                .longOpt("cli-config-path")
                .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path")
                .hasArg()
                .build();
            Option loggerOutputOption = Option.builder("l")
                .longOpt("logger-as-output")
                .desc("Cli output will be sent to logger instead of standard output")
                .build();

            cliCfgOption.setRequired(true);
            loggerOutputOption.setRequired(false);

            options.addOption(cliCfgOption);
            options.addOption(loggerOutputOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            ZooKeeperClient zkClient = null;
            boolean loggerAsOutput = cmd.hasOption("logger-as-output");
            try {
                String cliConfigPath = cmd.getOptionValue("cli-config-path");
                CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
                String zookeeperHostPorts = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
                String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
                int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                ZNode storeRoot = new ZNode(root, StoreMetadata.STORE_ZNODE_NAME);
                StoreMetadata storeMetadata = new StoreMetadata(zkClient, storeRoot);
                StoreParams storeParams = storeMetadata.getStoreParams();
                ZNode partitionRoot = new ZNode(storeRoot, StoreMetadata.PARTITION_ZNODE_NAME);

                listClusterInfoAndServerPartitionAssignments(zkClient, root);
                listStoreParams(storeRoot, storeParams);
                listReplicaAndGroupAssignments(storeRoot, storeMetadata.getReplicaAssignments(), storeMetadata.getGroupDescriptor());
                listConnections(storeRoot, storeMetadata.getConnectionMetadata());
                if (storeParams != null) {
                    for (int id = 0; id < storeParams.numPartitions; id++) {
                        ZNode znode = new ZNode(partitionRoot, Integer.toString(id));
                        Map<ReplicaId, ReplicaState> replicaState;
                        try {
                            PartitionMetadata partitionMetadata = zkClient.getData(znode, PartitionMetadataSerializer.INSTANCE).value;
                            replicaState = partitionMetadata.replicaStates;
                        } catch (KeeperException.NoNodeException e) {
                            replicaState = Collections.emptyMap();
                        }
                        listReplicaState(znode, replicaState);
                    }
                }
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
                if (loggerAsOutput) {
                    Logger logger = Logging.getLogger(ListZk.class);
                    logger.info(OUTPUT_BUILDER.toString());
                } else {
                    System.out.println(OUTPUT_BUILDER);
                }
            }
        }

        private void listStoreParams(ZNode storeRoot, StoreParams storeParams) {
            OUTPUT_BUILDER.append(String.format("store [%s] parameters:%n", storeRoot));

            if (storeParams != null) {
                OUTPUT_BUILDER.append(String.format("  key=%s%n", storeParams.key.toString()));
                OUTPUT_BUILDER.append(String.format("  numPartitions=%s%n", storeParams.numPartitions));
            } else {
                OUTPUT_BUILDER.append(String.format("Store parameters not found%n"));
            }
        }

        private void listReplicaAndGroupAssignments(ZNode storeRoot, ReplicaAssignments replicaAssignments, GroupDescriptor groupDescriptor) throws Exception {
            OUTPUT_BUILDER.append(String.format("store [%s] replica and group assignments:%n", storeRoot));

            if ((replicaAssignments != null) && (groupDescriptor != null)) {
                Map<String, int[]> replicas = new TreeMap<>(replicaAssignments.replicas);
                Map<String, Integer> groups = groupDescriptor.groups;
                for (Map.Entry<String, int[]> entry : replicas.entrySet()) {
                    OUTPUT_BUILDER.append(String.format("  %s = %s, GroupId: %s%n", entry.getKey(), Arrays.toString(entry.getValue()), groups.get(entry.getKey())));
                }
            } else {
                OUTPUT_BUILDER.append(String.format("Replicas not found%n"));
            }
        }

        private void listConnections(ZNode storeRoot, ConnectionMetadata connectionMetadata) {
            OUTPUT_BUILDER.append(String.format("store [%s] connections:%n", storeRoot));

            if ((connectionMetadata != null)) {
                Map<String, Integer> connections = connectionMetadata.connections;
                for (Map.Entry<String, Integer> entry : connections.entrySet()) {
                    OUTPUT_BUILDER.append(String.format("  %s has admin port: %s%n", entry.getKey(), entry.getValue()));
                }
            } else {
                OUTPUT_BUILDER.append(String.format("Connections not found%n"));
            }
        }

        private void listClusterInfoAndServerPartitionAssignments(ZooKeeperClient zkClient, ZNode root) throws Exception {
            OUTPUT_BUILDER.append(ListCluster.list(root, zkClient));
        }

        private void listReplicaState(ZNode znode, Map<ReplicaId, ReplicaState> replicaState) {
            OUTPUT_BUILDER.append(String.format("store [%s] replica states:%n", znode));

            if (replicaState.size() > 0) {
                Map<ReplicaId, ReplicaState> sortedReplicaState = new TreeMap<>(replicaState);
                for (Map.Entry<ReplicaId, ReplicaState> entry : sortedReplicaState.entrySet()) {
                    OUTPUT_BUILDER.append(String.format("  %s, SessionId: %s, closingHighWaterMark: %s%n",
                        entry.getKey(), entry.getValue().sessionId, ((entry.getValue().closingHighWaterMark) == ReplicaState.UNRESOLVED ? "UNRESOLVED" : entry.getValue().closingHighWaterMark)));
                }
            } else {
                OUTPUT_BUILDER.append(String.format("No node found%n"));
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code clusterKey} command displays the cluster key to the console.
     */
    private static final class ShowClusterKey extends Cli {
        private static final String NAME = "show-cluster-key";
        private static final String DESCRIPTION = "Displays the cluster key.";

        protected ShowClusterKey(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option cliCfgOption = Option.builder("c")
                .longOpt("cli-config-path")
                .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path")
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                ZNode storeRoot = new ZNode(root, StoreMetadata.STORE_ZNODE_NAME);
                StoreMetadata storeMetadata = new StoreMetadata(zkClient, storeRoot);
                StoreParams storeParams = storeMetadata.getStoreParams();

                System.out.println(storeParams.key.toString());

            } catch (Throwable e) {
                System.out.println();

            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    public static final class Create extends Cli {
        private static final String NAME = "create";
        private static final String DESCRIPTION = "Creates Waltz server metadata.";

        private Create(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option nameOption = Option.builder("n")
                    .longOpt("name")
                    .desc("Specify the name of the Waltz cluster")
                    .hasArg()
                    .build();
            Option partitionsOption = Option.builder("p")
                    .longOpt("partitions")
                    .desc("Specify the number of partitions in the Waltz cluster")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path")
                    .hasArg()
                    .build();

            cliCfgOption.setRequired(true);
            nameOption.setRequired(true);
            partitionsOption.setRequired(true);

            options.addOption(cliCfgOption);
            options.addOption(nameOption);
            options.addOption(partitionsOption);
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                String clusterName = cmd.getOptionValue("name");
                int numPartitions = Integer.parseInt(cmd.getOptionValue("partitions"));
                if (numPartitions < 0) {
                    printErrorAndExit("Number of partitions must be a non-negative integer");
                }

                createCluster(zkClient, root, clusterName, numPartitions);
                createStores(zkClient, root, numPartitions);
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        public static void createCluster(ZooKeeperClient zkClient, ZNode root, String clusterName, int numPartitions) throws Exception {
            System.out.println("Verifying cluster does not already exist...");

            if (zkClient.exists(root) != null) {
                System.out.println("Cluster already exists. Aborting.");
                System.exit(1);
            }

            System.out.println("Setting the cluster parameters...");
            CreateCluster.create(root, clusterName, numPartitions, zkClient, false, null);

            ClusterManager clusterManager = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());
            System.out.println("The cluster parameters are set.");
            System.out.println("  name=" + clusterManager.clusterName());
            System.out.println("  numPartitions=" + clusterManager.numPartitions());
        }

        /**
         * A function to create store Znode, group Znode and assignment Znode in Zookeeper.
         * The created assignment and connection Znode will be empty.
         * @param zkClient
         * @param root
         * @param numPartitions
         * @throws Exception
         */
        public static void createStores(ZooKeeperClient zkClient,
                                        ZNode root,
                                        int numPartitions) throws Exception {
            createStores(zkClient, root, numPartitions, Collections.emptyMap(), Collections.emptyMap());
        }

        /**
         * A function to create store Znode, group Znode and assignment Znode in Zookeeper.
         * The assignment Znode will be enriched based on storageServerLocations.
         * It is useful to DemoSever and SmokeTest.
         * @param zkClient
         * @param root
         * @param numPartitions
         * @param storageGroups
         * @param connectionMetadata
         * @throws Exception
         */
        public static void createStores(ZooKeeperClient zkClient,
                                        ZNode root,
                                        int numPartitions,
                                        Map<String, Integer> storageGroups,
                                        Map<String, Integer> connectionMetadata) throws Exception {
            StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
            storeMetadata.create(numPartitions, storageGroups, connectionMetadata);
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    private static final class Delete extends Cli {
        private static final String NAME = "delete";
        private static final String DESCRIPTION = "Delete server metadata.";

        private Delete(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option nameOption = Option.builder("n")
                    .longOpt("name")
                    .desc("Specify the name of the Waltz cluster")
                    .hasArg()
                    .build();
            Option forceOption = Option.builder("f")
                    .longOpt("force")
                    .desc("Delete cluster even if cluster names don't match")
                    .hasArg(false)
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path")
                    .hasArg()
                    .build();

            cliCfgOption.setRequired(true);
            nameOption.setRequired(true);
            forceOption.setRequired(false);

            options.addOption(cliCfgOption);
            options.addOption(nameOption);
            options.addOption(forceOption);
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                String clusterName = cmd.getOptionValue("name");
                boolean force = cmd.hasOption("force");

                System.out.println("Running deletion for cluster [" + clusterName + "] ...");
                deleteCluster(zkClient, root, clusterName, force);
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        private void deleteCluster(ZooKeeperClient zkClient, ZNode root,
                                   String clusterName, boolean force) throws Exception {
            if (zkClient.exists(root) == null) {
                System.out.println("cluster root [" + root + "] does not exist");
            } else {
                ClusterParamsSerializer serializer = new ClusterParamsSerializer();
                NodeData<ClusterParams> nodeData = zkClient.getData(root, serializer);
                if (nodeData.value != null) {
                    if (force || clusterName.equals(nodeData.value.name)) {
                        zkClient.deleteRecursively(root);
                    } else {
                        System.out.println("specified cluster name [" + clusterName
                                + "] does not match zookeeper value [" + nodeData.value.name + "]");
                    }
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code Add} command adds storage node to cluster.
     */
    private static final class Add extends Cli {
        private static final String NAME = "add-storage-node";
        private static final String DESCRIPTION = "Add a storage node to a group.";

        private Add(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify the storage to add, in format of host:port")
                    .hasArg()
                    .build();
            Option storageAdminPortOption = Option.builder("a")
                    .longOpt("storage-admin-port")
                    .desc("Specify the admin port of the storage node to add.")
                    .hasArg()
                    .build();
            Option groupOption = Option.builder("g")
                    .longOpt("group")
                    .desc("Specify the group to add to")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path")
                    .hasArg()
                    .build();

            cliCfgOption.setRequired(true);
            storageOption.setRequired(true);
            storageAdminPortOption.setRequired(true);
            groupOption.setRequired(true);

            options.addOption(cliCfgOption);
            options.addOption(storageOption);
            options.addOption(storageAdminPortOption);
            options.addOption(groupOption);
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                String storage = cmd.getOptionValue("storage");
                int adminPort = Integer.parseInt(cmd.getOptionValue("storage-admin-port"));
                int groupId = Integer.parseInt(cmd.getOptionValue("group"));

                String[] storageHostAndPort = storage.split(":");
                if (storageHostAndPort.length != 2) {
                    throw new IllegalArgumentException("Storage must be in format of host:port");
                }

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
                storeMetadata.addStorageNode(storage, groupId, adminPort);
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code Remove} command removes storage node from cluster.
     */
    private static final class Remove extends Cli {
        private static final String NAME = "remove-storage-node";
        private static final String DESCRIPTION = "Remove a storage node from cluster.";

        private Remove(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify the storage to remove, in format of host:port")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path")
                    .hasArg()
                    .build();

            cliCfgOption.setRequired(true);
            storageOption.setRequired(true);

            options.addOption(cliCfgOption);
            options.addOption(storageOption);
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                String storage = cmd.getOptionValue("storage");

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
                storeMetadata.removeStorageNode(storage);
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code Assign} command assign partition to storage node.
     */
    private static final class Assign extends Cli {
        private static final String NAME = "assign-partition";
        private static final String DESCRIPTION = "Assign a partition or multiple partitions to a storage node.";

        private Assign(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition to assign or multiple partitions as comma-separated int ranges such as 0-6,7,10-16")
                    .hasArg()
                    .build();
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify the storage to be assigned to, in format of host:port")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path")
                    .hasArg()
                    .build();

            cliCfgOption.setRequired(true);
            partitionOption.setRequired(true);
            storageOption.setRequired(true);

            options.addOption(cliCfgOption);
            options.addOption(partitionOption);
            options.addOption(storageOption);
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                List<Integer> partitions = CliUtils.parseIntRanges(cmd.getOptionValue("partition"));

                String storage = cmd.getOptionValue("storage");
                StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
                storeMetadata.addPartitions(partitions, storage);
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code Unassign} command un-assign partition from storage node.
     */
    private static final class Unassign extends Cli {
        private static final String NAME = "unassign-partition";
        private static final String DESCRIPTION = "Un-assign a partition or multiple partitions from a storage node";

        private Unassign(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition to un-assign or multiple partitions as comma-separated int ranges such as 0-6,7,10-16")
                    .hasArg()
                    .build();
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify the storage to be un-assigned from, in format of host:port")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path")
                    .hasArg()
                    .build();

            cliCfgOption.setRequired(true);
            partitionOption.setRequired(true);
            storageOption.setRequired(true);

            options.addOption(cliCfgOption);
            options.addOption(partitionOption);
            options.addOption(storageOption);
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                List<Integer> partitions = CliUtils.parseIntRanges(cmd.getOptionValue("partition"));
                String storage = cmd.getOptionValue("storage");

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
                storeMetadata.removePartitions(partitions, storage);
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code AutoAssign} command automatically assign partitions to storage nodes in group.
     */
    private static final class AutoAssign extends Cli {
        private static final String NAME = "auto-assign";
        private static final String DESCRIPTION = "Automatically assign partitions to storage nodes in group.";

        private AutoAssign(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option groupOption = Option.builder("g")
                    .longOpt("group")
                    .desc("Specify the group to assign to")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path")
                    .hasArg()
                    .build();

            cliCfgOption.setRequired(true);
            groupOption.setRequired(true);

            options.addOption(cliCfgOption);
            options.addOption(groupOption);
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
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                int groupId = Integer.parseInt(cmd.getOptionValue("group"));

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
                storeMetadata.autoAssignPartition(groupId);
            } catch (Exception e) {
                if (zkClient != null) {
                    zkClient.close();
                }
                throw new SubCommandFailedException(e);
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    public static void testMain(String[] args) {
        new ZooKeeperCli(args, true).processCmd();
    }

    public static void main(String[] args) {
        new ZooKeeperCli(args, false).processCmd();
    }

}
