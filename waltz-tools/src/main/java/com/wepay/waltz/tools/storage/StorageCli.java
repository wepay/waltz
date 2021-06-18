package com.wepay.waltz.tools.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.storage.server.internal.PartitionInfoSnapshot;
import com.wepay.waltz.exception.StoreMetadataException;
import com.wepay.waltz.store.internal.ReplicaConnection;
import com.wepay.waltz.store.internal.ReplicaConnectionImpl;
import com.wepay.waltz.common.metadata.ConnectionMetadata;
import com.wepay.waltz.common.metadata.ConnectionMetadataSerializer;
import com.wepay.waltz.common.metadata.GroupDescriptorSerializer;
import com.wepay.waltz.common.metadata.ReplicaAssignments;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.metadata.StoreParams;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import io.netty.handler.ssl.SslContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * StorageCli is a tool for interacting with Waltz Storage.
 */
public final class StorageCli extends SubcommandCli {

    private StorageCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
                new Subcommand(ListPartition.NAME, ListPartition.DESCRIPTION, ListPartition::new),
                new Subcommand(AddPartition.NAME, AddPartition.DESCRIPTION, AddPartition::new),
                new Subcommand(RemovePartition.NAME, RemovePartition.DESCRIPTION, RemovePartition::new),
                new Subcommand(Availability.NAME, Availability.DESCRIPTION, Availability::new),
                new Subcommand(RecoverPartition.NAME, RecoverPartition.DESCRIPTION, RecoverPartition::new),
                new Subcommand(SyncPartitionAssignments.NAME, SyncPartitionAssignments.DESCRIPTION, SyncPartitionAssignments::new),
                new Subcommand(Validate.NAME, Validate.DESCRIPTION, Validate::new),
                new Subcommand(MaxTransactionId.NAME, MaxTransactionId.DESCRIPTION, MaxTransactionId::new)
        ));
    }

    /**
     * The {@code ListPartition} command lists all partition ids of a storage node.
     */
    private static final class ListPartition extends Cli {
        private static final String NAME = "list";
        private static final String DESCRIPTION = "List partition ownership data of a storage node";

        private static final String STORAGE_PARTITION_METRIC_KEY = "waltz-storage.waltz-storage-partition-ids";
        private final ObjectMapper mapper = new ObjectMapper();

        protected ListPartition(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify storage in format of host:admin_port")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path and SSL config")
                    .hasArg()
                    .build();
            Option loggerOutputOption = Option.builder("l")
                .longOpt("logger-as-output")
                .desc("Cli output will be sent to logger instead of standard output")
                .build();
            storageOption.setRequired(false);
            cliCfgOption.setRequired(true);
            loggerOutputOption.setRequired(false);
            options.addOption(storageOption);
            options.addOption(cliCfgOption);
            options.addOption(loggerOutputOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String cliConfigPath = cmd.getOptionValue("cli-config-path");
            String providedHostAndPort = cmd.getOptionValue("storage");
            List<String[]> hostsAndPorts;
            if (providedHostAndPort == null) {
                try {
                    hostsAndPorts = getAllHostsAndPorts(cliConfigPath);
                } catch (Exception e) {
                    throw new SubCommandFailedException("Cannot obtain zookeeper connections");
                }
            } else {
                try {
                    String[] hostAndPortArray = providedHostAndPort.split(":");
                    if (hostAndPortArray.length != 2) {
                        throw new IllegalArgumentException("Http must be in format of host:admin_port");
                    }
                    hostsAndPorts = Collections.singletonList(hostAndPortArray);
                } catch (IllegalArgumentException e) {
                    throw new SubCommandFailedException(String.format("Failed to obtain hostname and admin port for "
                            + "%s:%n%s", providedHostAndPort, e.getMessage()));
                }
            }

            boolean loggerAsOutput = cmd.hasOption("logger-as-output");
            StringBuilder partitionInfoStringBuilder = new StringBuilder();
            for (String[] hostAndPortArray : hostsAndPorts) {
                try {
                    String storageHost = hostAndPortArray[0];
                    String storagePort = hostAndPortArray[1];

                    String metricsJson = getMetricsJson(storageHost, Integer.parseInt(storagePort), cliConfigPath);
                    Map<Integer, PartitionInfoSnapshot> partitionInfo = getPartitionInfo(metricsJson);
                    partitionInfoStringBuilder.append(formatPartitionInfo(partitionInfo, storageHost, storagePort));
                    partitionInfoStringBuilder.append(System.lineSeparator());
                } catch (Exception e) {
                    throw new SubCommandFailedException(String.format("Cannot fetch partition ownership for %s:%s:%n%s",
                            hostAndPortArray[0], hostAndPortArray[1], e.getMessage()));
                }
            }
            if (loggerAsOutput) {
                Logger logger = Logging.getLogger(ListPartition.class);
                logger.info(partitionInfoStringBuilder.toString());
            } else {
                System.out.println(partitionInfoStringBuilder.toString());
            }
        }

        private List<String[]> getAllHostsAndPorts(String cliConfigPath) throws Exception {
            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            ZooKeeperClient zkClient = null;

            try {
                String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
                int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
                int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);
                String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(zkRoot + '/' + StoreMetadata.STORE_ZNODE_NAME));
                ConnectionMetadata md = storeMetadata.getConnectionMetadata();
                Map<String, Integer> cons = md.connections;

                List<String[]> hostsAndPorts = new ArrayList<>();
                for (Map.Entry<String, Integer> con : cons.entrySet()) {
                    hostsAndPorts.add(new String[]{con.getKey().split(":")[0], con.getValue().toString()});
                }
                return hostsAndPorts;
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        private String getMetricsJson(String storageHost, int storagePort, String cliConfigPath) throws Exception {
            ZooKeeperClient zkClient = null;
            StorageAdminClient storageAdminClient = null;
            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);
                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);
                storageAdminClient = openStorageAdminClient(storageHost, storagePort, sslContext, zkClient, zkRoot);
                return (String) storageAdminClient.getMetrics().get();
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
                if (storageAdminClient != null) {
                    storageAdminClient.close();
                }
            }
        }

        private Map<Integer, PartitionInfoSnapshot>  getPartitionInfo(String metricsJson) throws IOException {
            JsonNode metricsNode = mapper.readTree(metricsJson).path("gauges");
            Map<Integer, PartitionInfoSnapshot> partitionInfo = new HashMap<>();

            if (metricsNode.path(STORAGE_PARTITION_METRIC_KEY) != null) {
                JsonNode partitionIds = metricsNode.path(STORAGE_PARTITION_METRIC_KEY).path("value");

                Iterator<JsonNode> element = partitionIds.elements();
                while (element.hasNext()) {
                    Integer id = element.next().asInt();
                    PartitionInfoSnapshot partitionInfoSnapshot = new PartitionInfoSnapshot(
                            id,
                            metricsNode.path("waltz-storage.partition-" + id + ".session-id").path("value").asInt(),
                            metricsNode.path("waltz-storage.partition-" + id + ".low-water-mark").path("value").asInt(),
                            metricsNode.path("waltz-storage.partition-" + id + ".local-low-water-mark").path("value").asInt(),
                            metricsNode.path("waltz-storage.partition-" + id + ".flags").path("value").asInt()
                    );
                    partitionInfo.put(id, partitionInfoSnapshot);
                }
            }

            return partitionInfo;
        }

        private StringBuilder formatPartitionInfo(Map<Integer, PartitionInfoSnapshot> partitionInfo, String storageHost, String storagePort) {
            StringBuilder sb = new StringBuilder();
            // display partition info
            for (Map.Entry<Integer, PartitionInfoSnapshot> entry: partitionInfo.entrySet()) {
                int partitionId = entry.getKey();
                PartitionInfoSnapshot snapshot = entry.getValue();
                sb.append(String.format("Partition Info for id: %d at %s:%s%n", partitionId, storageHost, storagePort));
                sb.append(String.format("\t sessionId: %d%n", snapshot.sessionId));
                sb.append(String.format("\t lowWaterMark: %d%n", snapshot.lowWaterMark));
                sb.append(String.format("\t localLowWaterMark: %d%n", snapshot.localLowWaterMark));
                sb.append(String.format("\t isAssigned: %b%n", snapshot.isAssigned));
                sb.append(String.format("\t isAvailable: %b%n", snapshot.isAvailable));
            }
            return sb;
        }

        private void listPartitionInfo(Map<Integer, PartitionInfoSnapshot> partitionInfo, String storageHost, String storagePort) {
            System.out.println(formatPartitionInfo(partitionInfo, storageHost, storagePort));
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code AddPartition} command adds a partition ownership to a storage node.
     */
    private static final class AddPartition extends Cli {
        private static final String NAME = "add-partition";
        private static final String DESCRIPTION = "Add a partition ownership to a storage node";

        private AddPartition(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify storage in format of host:admin_port")
                    .hasArg()
                    .build();
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition id to be added to the storage node")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path and SSL config")
                    .hasArg()
                    .build();

            storageOption.setRequired(true);
            partitionOption.setRequired(true);
            cliCfgOption.setRequired(true);

            options.addOption(storageOption);
            options.addOption(partitionOption);
            options.addOption(cliCfgOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String hostAndPort = cmd.getOptionValue("storage");
            String partitionId = cmd.getOptionValue("partition");
            String cliConfigPath = cmd.getOptionValue("cli-config-path");

            try {
                String[] hostAndPortArray = hostAndPort.split(":");
                if (hostAndPortArray.length != 2) {
                    throw new IllegalArgumentException("Storage must be in format of host:admin_port");
                }
                String storageHost = hostAndPortArray[0];
                String storagePort = hostAndPortArray[1];

                if (!partitionId.matches("^[0-9]+$")) {
                    throw new IllegalArgumentException(String.format("Partition id '%s' is invalid. Expected a non-negative integer", partitionId));
                }

                addPartition(storageHost, Integer.parseInt(storagePort), Integer.parseInt(partitionId), cliConfigPath);
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to add partition %s. %n%s", partitionId, e.getMessage()));
            }
        }

        private void addPartition(String storageHost, int storagePort, int partitionId, String cliConfigPath) throws Exception {
            ZooKeeperClient zkClient = null;
            StorageAdminClient storageAdminClient = null;
            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);

                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);

                storageAdminClient = openStorageAdminClient(storageHost, storagePort, sslContext, zkClient, zkRoot);

                storageAdminClient.setPartitionAssignment(partitionId, true, false).get();
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
                if (storageAdminClient != null) {
                    storageAdminClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code Availability} command sets the availability of the partition in a storage node.
     */
    private static final class Availability extends Cli {
        private static final String NAME = "availability";
        private static final String DESCRIPTION = "Set the read/write availability of the partition in a storage node";

        private Availability(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify storage in format of host:admin_port")
                    .hasArg()
                    .build();
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition id to be added to the storage node")
                    .hasArg()
                    .build();
            Option onlineOption = Option.builder("o")
                    .longOpt("online")
                    .desc("Specify 'true' or 'false' for the storage node is online")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path and SSL config")
                    .hasArg()
                    .build();

            storageOption.setRequired(true);
            partitionOption.setRequired(true);
            onlineOption.setRequired(true);
            cliCfgOption.setRequired(true);

            options.addOption(storageOption);
            options.addOption(partitionOption);
            options.addOption(onlineOption);
            options.addOption(cliCfgOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String hostAndPort = cmd.getOptionValue("storage");
            String partitionId = cmd.getOptionValue("partition");
            String isOnline = cmd.getOptionValue("online");
            String cliConfigPath = cmd.getOptionValue("cli-config-path");

            try {
                String[] hostAndPortArray = hostAndPort.split(":");
                if (hostAndPortArray.length != 2) {
                    throw new IllegalArgumentException("Storage must be in format of host:admin_port");
                }
                String storageHost = hostAndPortArray[0];
                String storagePort = hostAndPortArray[1];

                if (!partitionId.matches("^[0-9]+$")) {
                    throw new IllegalArgumentException("Partition id must be a non-negative integer");
                }

                if (isOnline != null && !isOnline.equals("true") && !isOnline.equals("false")) {
                    throw new IllegalArgumentException("--online must be set to 'true' or 'false'");
                }

                setAvailability(storageHost, Integer.parseInt(storagePort), Integer.parseInt(partitionId), Boolean.parseBoolean(isOnline), cliConfigPath);
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to set availability of partition %s to %s "
                        + "for %s. %n%s", partitionId, isOnline, hostAndPort, e.getMessage()));
            }
        }

        private void setAvailability(String storageHost, int storagePort, int partitionId, boolean isAvailable, String cliConfigPath) throws Exception {
            ZooKeeperClient zkClient = null;
            StorageAdminClient storageAdminClient = null;
            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);
                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);
                storageAdminClient = openStorageAdminClient(storageHost, storagePort, sslContext, zkClient, zkRoot);
                storageAdminClient.setPartitionAvailable(partitionId, isAvailable).get();
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
                if (storageAdminClient != null) {
                    storageAdminClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }


    /**
     * The {@code RemovePartition} command removes a partition ownership from a storage node
     * and deletes all data segment files for that partition.
     */
    private static final class RemovePartition extends Cli {
        private static final String NAME = "remove-partition";
        private static final String DESCRIPTION = "Remove a partition ownership from a storage node and deletes corresponding data segment files";

        private RemovePartition(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify storage in format of host:admin_port")
                    .hasArg()
                    .build();
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition id to be removed from the storage node")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config")
                    .hasArg()
                    .build();
            Option deleteStorageFilesOption = Option.builder("d")
                    .longOpt("delete-storage-files")
                    .desc("Storage files of this partition will be deleted.")
                    .build();

            storageOption.setRequired(true);
            partitionOption.setRequired(true);
            cliCfgOption.setRequired(true);
            deleteStorageFilesOption.setRequired(false);

            options.addOption(storageOption);
            options.addOption(partitionOption);
            options.addOption(cliCfgOption);
            options.addOption(deleteStorageFilesOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String hostAndPort = cmd.getOptionValue("storage");
            String partitionId = cmd.getOptionValue("partition");
            String cliConfigPath = cmd.getOptionValue("cli-config-path");
            boolean deleteStorageFiles = false;

            if (cmd.hasOption("delete-storage-files")) {
                deleteStorageFiles = true;
            }

            try {
                String[] hostAndPortArray = hostAndPort.split(":");
                if (hostAndPortArray.length != 2) {
                    throw new IllegalArgumentException("Storage must be in format of host:admin_port");
                }
                String storageHost = hostAndPortArray[0];
                String storagePort = hostAndPortArray[1];

                if (!partitionId.matches("^[0-9]+$")) {
                    throw new IllegalArgumentException("Partition id must be a non-negative integer");
                }

                removePartition(storageHost, Integer.parseInt(storagePort), Integer.parseInt(partitionId), cliConfigPath, deleteStorageFiles);
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to remove partition %s from %s. %n%s",
                    partitionId, hostAndPort, e.getMessage()));
            }
        }

        /**
         * Remove a partition ownership from a storage node and deletes the corresponding data segment files.
         * This operation is IRREVERSIBLE: It will delete data segment files.
         *
         * @param storageHost                   storage host
         * @param storagePort                   storage port
         * @param partitionId                   the partition id
         * @param cliConfigPath                 the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config file path
         * @param deleteStorageFiles            Determines whether to delete the storage files within the partition or not
         * @throws Exception
         */
        private void removePartition(String storageHost, int storagePort, int partitionId, String cliConfigPath, boolean deleteStorageFiles) throws Exception {
            ZooKeeperClient zkClient = null;
            StorageAdminClient storageAdminClient = null;
            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);

                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);

                storageAdminClient = openStorageAdminClient(storageHost, storagePort, sslContext, zkClient, zkRoot);

                storageAdminClient.setPartitionAssignment(partitionId, false, deleteStorageFiles).get();
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
                if (storageAdminClient != null) {
                    storageAdminClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code RecoverPartition} command loads data from a partition into a storage node.
     * The command operates by connecting to Waltz server as a consumer, consuming data for the partition,
     * and inserting it into the storage node via a {@code StorageClient}. If the storage node already has
     * some data for the partition on it, the recovery will pick up from the node's low watermark.
     */
    private static final class RecoverPartition extends Cli {
        private static final String NAME = "recover-partition";
        private static final String DESCRIPTION = "Copies records for a partition from one storage node to another";

        private RecoverPartition(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option sourceStorageOption = Option.builder("s")
                    .longOpt("source-storage")
                    .desc("Specify source storage in format of host:admin_port")
                    .hasArg()
                    .build();
            Option destinationStorageOption = Option.builder("d")
                    .longOpt("destination-storage")
                    .desc("Specify destination storage in format of host:admin_port")
                    .hasArg()
                    .build();
            Option destinationStoragePortOption = Option.builder("dp")
                    .longOpt("destination-storage-port")
                    .desc("Specify the port of destination storage, where port is non-admin port")
                    .hasArg()
                    .build();
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition id to be recovered")
                    .hasArg()
                    .build();
            Option batchSizeOption = Option.builder("b")
                    .longOpt("batch-size")
                    .desc("Specify the batch size to use when fetching records from storage node")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path")
                    .hasArg()
                    .build();
            Option sourceSslOption = Option.builder("sssl")
                    .longOpt("source-ssl-config-path")
                    .desc("Specify the SSL config file path required for the source storage node")
                    .hasArg()
                    .build();
            Option destinationSslOption = Option.builder("dssl")
                    .longOpt("destination-ssl-config-path")
                    .desc("Specify the SSL config file path required for the destination storage node")
                    .hasArg()
                    .build();

            sourceStorageOption.setRequired(true);
            destinationStorageOption.setRequired(true);
            destinationStoragePortOption.setRequired(true);
            partitionOption.setRequired(true);
            batchSizeOption.setRequired(true);
            cliCfgOption.setRequired(true);
            sourceSslOption.setRequired(false);
            destinationSslOption.setRequired(false);

            options.addOption(sourceStorageOption);
            options.addOption(destinationStorageOption);
            options.addOption(destinationStoragePortOption);
            options.addOption(partitionOption);
            options.addOption(batchSizeOption);
            options.addOption(cliCfgOption);
            options.addOption(sourceSslOption);
            options.addOption(destinationSslOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String sourceHostAndAdminPort = cmd.getOptionValue("source-storage");
            String destinationHostAndAdminPort = cmd.getOptionValue("destination-storage");
            String destinationStoragePort = cmd.getOptionValue("destination-storage-port");
            String partitionId = cmd.getOptionValue("partition");
            String batchSize = cmd.getOptionValue("batch-size");
            String cliConfigPath = cmd.getOptionValue("cli-config-path");
            String sourceSslConfigPath = cmd.getOptionValue("source-ssl-config-path");
            String destinationSslConfigPath = cmd.getOptionValue("destination-ssl-config-path");

            try {
                String[] sourceHostAndAdminPortArray = sourceHostAndAdminPort.split(":");
                String[] destinationHostAndAdminPortArray = destinationHostAndAdminPort.split(":");
                if (sourceHostAndAdminPortArray.length != 2 || destinationHostAndAdminPortArray.length != 2) {
                    throw new IllegalArgumentException("Source and destination storage must both be in format of host:admin_port");
                }
                String sourceStorageHost = sourceHostAndAdminPortArray[0];
                String sourceStorageAdminPort = sourceHostAndAdminPortArray[1];
                String destinationStorageHost = destinationHostAndAdminPortArray[0];
                String destinationStorageAdminPort = destinationHostAndAdminPortArray[1];

                if (!partitionId.matches("^[0-9]+$")) {
                    throw new IllegalArgumentException("Partition id must be a non-negative integer");
                }

                if (!batchSize.matches("^[0-9]+$")) {
                    throw new IllegalArgumentException("Batch size must be a non-negative integer");
                }

                recoverPartition(sourceStorageHost, Integer.parseInt(sourceStorageAdminPort), destinationStorageHost, Integer.parseInt(destinationStorageAdminPort),
                                 Integer.parseInt(destinationStoragePort), Integer.parseInt(partitionId), Integer.parseInt(batchSize), cliConfigPath,
                                 sourceSslConfigPath, destinationSslConfigPath);
           } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Partition %s failed to recover. %n%s", partitionId, e.getMessage()));
            }
        }

        /**
         * Recovers a partition on a storage node by consuming messages from Waltz server, and writing them
         * to the storage node. The recovery will pick up from the storage node's low watermark.
         *
         * @param sourceStorageHost             source storage host
         * @param sourceStorageAdminPort        source storage admin port
         * @param destinationStorageHost        destination storage host
         * @param destinationStoragePort        destination storage admin port
         * @param destinationStoragePort        destination storage port
         * @param partitionId                   the partition id
         * @param batchSize                     the batch size to use when fetching records from storage node
         * @param cliConfigPath                 the cli config file path required for ZooKeeper connection string, ZooKeeper root path
         * @param sourceSslConfigPath           the SSL config file path required for the source storage node
         * @param destinationSslConfigPath      the SSL config file path required for the destination storage node
         * @throws Exception
         */
        private void recoverPartition(String sourceStorageHost, int sourceStorageAdminPort, String destinationStorageHost, int destinationStorageAdminPort,
                                      int destinationStoragePort, int partitionId, int batchSize, String cliConfigPath, String sourceSslConfigPath,
                                      String destinationSslConfigPath) throws Exception {
            ZooKeeperClient zkClient = null;
            StorageAdminClient sourceStorageAdminClient = null;
            StorageClient destinationStorageClient = null;
            StorageAdminClient destinationStorageAdminClient = null;

            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

            try {
                SslContext sourceSslContext = Utils.getSslContext(sourceSslConfigPath, WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
                SslContext destinationSslContext = Utils.getSslContext(destinationSslConfigPath, WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);

                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);
                sourceStorageAdminClient = openStorageAdminClient(sourceStorageHost, sourceStorageAdminPort, sourceSslContext, zkClient, zkRoot);
                destinationStorageClient = openStorageClient(destinationStorageHost, destinationStoragePort, destinationSslContext, zkClient, zkRoot, true);
                destinationStorageAdminClient = openStorageAdminClient(destinationStorageHost, destinationStorageAdminPort, destinationSslContext, zkClient, zkRoot);

                StorageRecoveryRunnable storageRecoveryRunnable = new StorageRecoveryRunnable(sourceStorageAdminClient, destinationStorageAdminClient, destinationStorageClient, partitionId, batchSize);

                storageRecoveryRunnable.run();
            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
                if (sourceStorageAdminClient != null) {
                    sourceStorageAdminClient.close();
                }
                if (destinationStorageClient != null) {
                    destinationStorageClient.close();
                }
                if (destinationStorageAdminClient != null) {
                    destinationStorageAdminClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * The {@code SyncPartitionAssignments} command sync partition ownership to storage nodes based on assignment specified in ZooKeeper.
     */
    private static final class SyncPartitionAssignments extends Cli {
        private static final String NAME = "sync-partitions";
        private static final String DESCRIPTION = "Sync partition ownership to storage nodes based on assignment specified in ZooKeeper";

        private SyncPartitionAssignments(String[] args) {
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
            String cliConfigPath = cmd.getOptionValue("cli-config-path");

            try {
                syncPartitionAssignments(cliConfigPath);
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Partition assignment cannot be fully synced. %n%s", e.getMessage()));
            }
        }

        @SuppressWarnings("unchecked")
        private void syncPartitionAssignments(String cliConfigPath) throws Exception {
            ZooKeeperClient zkClient = null;

            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);
                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);

                StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
                ReplicaAssignments assignments = storeMetadata.getReplicaAssignments();
                ConnectionMetadata connectionMetadata = storeMetadata.getConnectionMetadata();
                for (Map.Entry<String, int[]> assignment : assignments.replicas.entrySet()) {
                    String storageConnectString = assignment.getKey();
                    String storageHost = storageConnectString.split(":")[0];
                    int[] partitionIds = assignment.getValue();
                    int storageAdminPort = connectionMetadata.connections.get(storageConnectString);
                    StorageAdminClient storageAdminClient = null;

                    try {
                        List<CompletableFuture> futures = new ArrayList<>();
                        storageAdminClient = openStorageAdminClient(storageHost, storageAdminPort, sslContext,
                            zkClient, zkRoot);

                        // Get current assigned partitions.
                        HashMap<Integer, Boolean> assignedPartitionStatus =
                            (HashMap<Integer, Boolean>) storageAdminClient.getAssignedPartitionStatus().get();

                        for (int partitionId : partitionIds) {
                            if ((assignedPartitionStatus.get(partitionId) == null) || !assignedPartitionStatus.get(partitionId)) {
                                futures.add(storageAdminClient.setPartitionAssignment(partitionId, true, false));
                            }
                            assignedPartitionStatus.remove(partitionId);
                        }

                        // Remove the remaining old partitions that are no longer to be handled.
                        for (int partitionId : assignedPartitionStatus.keySet()) {
                            futures.add(storageAdminClient.setPartitionAssignment(partitionId, false, false));
                        }

                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
                    } finally {
                        if (storageAdminClient != null) {
                            storageAdminClient.close();
                        }
                    }
                }
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
     * The {@code Validate} command validate Waltz storage node connectivity.
     */
    private static final class Validate extends Cli {
        private static final String NAME = "validate";
        private static final String DESCRIPTION = "Validate Waltz storage node connectivity";

        private Validate(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path and SSL config")
                    .hasArg()
                    .build();
            cliCfgOption.setRequired(true);

            options.addOption(cliCfgOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                String cliConfigPath = cmd.getOptionValue("cli-config-path");

                validateConnectivity(cliConfigPath);
            } catch (Exception e) {
                throw new SubCommandFailedException(e.getMessage());
            }
        }

        private void validateConnectivity(String cliConfigPath) throws Exception {
            ZooKeeperClient zkClient = null;

            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zookeeperHostPorts = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);

            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);
                zkClient = new ZooKeeperClientImpl(zookeeperHostPorts, zkSessionTimeout, zkConnectTimeout);
                ZNode root = new ZNode(zkRoot);
                ZNode storeRoot = new ZNode(root, StoreMetadata.STORE_ZNODE_NAME);

                // groups is a map of <hostname+port: groupId>
                Map<String, Integer> groups = zkClient.getData(new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME), GroupDescriptorSerializer.INSTANCE).value.groups;
                Set<String> connectionsWithPort = groups.keySet();
                validatePortConnectivity(connectionsWithPort, sslContext, zkClient, zkRoot);

                // connections is a map of <hostname+port: adminPort>
                Map<String, Integer> connections = zkClient.getData(new ZNode(storeRoot, StoreMetadata.CONNECTION_ZNODE_NAME), ConnectionMetadataSerializer.INSTANCE).value.connections;
                Set<String> connectionsWithAdminPort = connections.keySet().stream().map(
                        key -> key.replaceAll("\\d+$", String.valueOf(connections.get(key))))
                        .collect(Collectors.toSet());
                validateAdminPortConnectivity(connectionsWithAdminPort, sslContext, zkClient, zkRoot);

            } finally {
                if (zkClient != null) {
                    zkClient.close();
                }
            }
        }

        private void validatePortConnectivity(Set<String> connections, SslContext sslContext, ZooKeeperClient zkClient, String zkRoot) throws Exception {
            StorageClient storageClient = null;
            for (String connection : connections) {
                String[] hostAndPortArray = connection.split(":");
                String host = hostAndPortArray[0];
                int port = Integer.parseInt(hostAndPortArray[1]);
                try {
                    storageClient = openStorageClient(host, port, sslContext, zkClient, zkRoot);
                    storageClient.awaitOpen();
                } catch (Exception e) {
                    throw new Exception(String.format("Invalid hostname or port: %s", e.getMessage()));
                } finally {
                    if (storageClient != null) {
                        storageClient.close();
                    }
                }
            }
        }

        private void validateAdminPortConnectivity(Set<String> connections, SslContext sslContext, ZooKeeperClient zkClient, String zkRoot) throws Exception {
            StorageAdminClient storageAdminClient = null;
            for (String connection : connections) {
                String[] hostAndPortArray = connection.split(":");
                String host = hostAndPortArray[0];
                int port = Integer.parseInt(hostAndPortArray[1]);
                try {
                    storageAdminClient = openStorageAdminClient(host, port, sslContext, zkClient, zkRoot);
                    storageAdminClient.awaitOpen();
                } catch (Exception e) {
                    throw new Exception(String.format("Invalid hostname or admin port: %s", e.getMessage()));
                } finally {
                    if (storageAdminClient != null) {
                        storageAdminClient.close();
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
     * The {@code MaxTransactionId} command displays the maximum transaction ID of a partition for given storage node.
     */
    private static final class MaxTransactionId extends Cli {
        private static final String NAME = "max-transaction-id";
        private static final String DESCRIPTION = "Displays the max transaction ID";

        private MaxTransactionId(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option storageOption = Option.builder("s")
                    .longOpt("storage")
                    .desc("Specify storage in format of host:admin_port")
                    .hasArg()
                    .build();
            Option storagePortOption = Option.builder("sp")
                    .longOpt("storage-port")
                    .desc("Specify the port of storage, where port is non-admin port")
                    .hasArg()
                    .build();
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition id whose max transaction ID to be returned")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config")
                    .hasArg()
                    .build();
            Option offlineOption = Option.builder("o")
                    .longOpt("offline")
                    .desc("Check max transaction ID when storage is offline")
                    .hasArg(false)
                    .build();
            storageOption.setRequired(true);
            storagePortOption.setRequired(true);
            partitionOption.setRequired(true);
            cliCfgOption.setRequired(true);
            offlineOption.setRequired(false);

            options.addOption(storageOption);
            options.addOption(storagePortOption);
            options.addOption(partitionOption);
            options.addOption(cliCfgOption);
            options.addOption(offlineOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String hostAndPort = cmd.getOptionValue("storage");
            String storagePort = cmd.getOptionValue("storage-port");
            String partitionId = cmd.getOptionValue("partition");
            String cliConfigPath = cmd.getOptionValue("cli-config-path");
            boolean usedByOfflineRecovery = cmd.hasOption("offline");

            try {
                String[] hostAndPortArray = hostAndPort.split(":");
                if (hostAndPortArray.length != 2) {
                    throw new IllegalArgumentException("Storage must be in format of host:admin_port");
                }
                String storageHost = hostAndPortArray[0];
                String storageAdminPort = hostAndPortArray[1];

                long maxTransactionId = getMaxTransactionId(storageHost, Integer.parseInt(storagePort), Integer.parseInt(storageAdminPort), Integer.parseInt(partitionId), cliConfigPath, usedByOfflineRecovery);
                System.out.println("Max Transaction ID: " + maxTransactionId);
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to read max transaction ID for storage %s. %n%s", hostAndPort, e.getMessage()));
            }
        }

        /**
         * Return max transaction ID of a partition read from storage.
         * @param storageHost       storage host
         * @param storagePort       storage port
         * @param storageAdminPort  storage admin port
         * @param cliConfigPath     the cli config file path required for zooKeeper connection string, zooKeeper root path
         * @return max transaction ID
         */
        private long getMaxTransactionId(String storageHost, int storagePort, int storageAdminPort, int partitionId, String cliConfigPath, boolean isOffline) throws Exception {
            ZooKeeperClient zkClient = null;
            StorageClient storageClient = null;
            StorageAdminClient storageAdminClient = null;
            ReplicaConnection destinationReplicaConnection = null;

            CliConfig cliConfig = CliConfig.parseCliConfigFile(cliConfigPath);
            String zkConnectString = (String) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_STRING);
            String zkRoot = (String) cliConfig.get(CliConfig.CLUSTER_ROOT);
            int zkSessionTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_SESSION_TIMEOUT);
            int zkConnectTimeout = (int) cliConfig.get(CliConfig.ZOOKEEPER_CONNECT_TIMEOUT);
            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);
                zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout, zkConnectTimeout);

                storageClient = openStorageClient(storageHost, storagePort, sslContext, zkClient, zkRoot, isOffline);
                storageAdminClient = openStorageAdminClient(storageHost, storageAdminPort, sslContext, zkClient, zkRoot);

                SessionInfo sessionInfo = (SessionInfo) storageAdminClient.lastSessionInfo(partitionId).get();
                long sessionId = sessionInfo.sessionId;

                // Create destination storage connection
                destinationReplicaConnection = new ReplicaConnectionImpl(partitionId, sessionId, storageClient);

                // Get destination storage current high watermark
                return destinationReplicaConnection.getMaxTransactionId();

            } finally {
                if (destinationReplicaConnection != null) {
                    destinationReplicaConnection.close();
                }
                if (zkClient != null) {
                    zkClient.close();
                }
                if (storageClient != null) {
                    storageClient.close();
                }
                if (storageAdminClient != null) {
                    storageAdminClient.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * Helper method to create and open a StorageClient
     *
     * @param host       storage host
     * @param port       storage port
     * @param sslContext the SslContext used to instantiate the StorageClient
     * @param zkClient   the ZooKeeperClient used for the Waltz Cluster
     * @param root       the ZooKeeper root path to the Waltz cluster
     * @return StorageClient
     */
    private static StorageClient openStorageClient(String host, int port, SslContext sslContext, ZooKeeperClient zkClient, String root)
            throws StoreMetadataException {
        return openStorageClient(host, port, sslContext, zkClient, root, false);
    }

    /**
     * Helper method to create and open a StorageClient
     *
     * @param host                  storage host
     * @param port                  storage port
     * @param sslContext            the SslContext used to instantiate the StorageClient
     * @param zkClient              the ZooKeeperClient used for the Waltz Cluster
     * @param root                  the ZooKeeper root path to the Waltz cluster
     * @param usedByOfflineRecovery if the client is used by offline recovery
     * @return StorageClient
     */
    private static StorageClient openStorageClient(String host, int port, SslContext sslContext, ZooKeeperClient zkClient, String root, boolean usedByOfflineRecovery)
            throws StoreMetadataException {

        StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root + '/' + StoreMetadata.STORE_ZNODE_NAME));
        StoreParams storeParams = storeMetadata.getStoreParams();

        StorageClient storageClient = new StorageClient(host, port, sslContext, storeParams.key, storeParams.numPartitions, usedByOfflineRecovery);
        storageClient.open();

        return storageClient;
    }

    /**
     * Helper method to create and open a StorageAdminClient
     *
     * @param host       storage host
     * @param port       storage port
     * @param sslContext the SslContext used to instantiate the StorageClient
     * @param zkClient   the ZooKeeperClient used for the Waltz Cluster
     * @param root       the ZooKeeper root path to the Waltz cluster
     * @return StorageClient
     */
    private static StorageAdminClient openStorageAdminClient(String host, int port, SslContext sslContext, ZooKeeperClient zkClient, String root)
            throws StoreMetadataException, StorageRpcException {

        StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root + '/' + StoreMetadata.STORE_ZNODE_NAME));
        StoreParams storeParams = storeMetadata.getStoreParams();

        StorageAdminClient storageAdminClient = new StorageAdminClient(host, port, sslContext, storeParams.key, storeParams.numPartitions);
        storageAdminClient.open();

        return storageAdminClient;
    }

    public static void testMain(String[] args)  {
        new StorageCli(args, true).processCmd();
    }

    public static void main(String[] args) {
        new StorageCli(args, false).processCmd();
    }
}
