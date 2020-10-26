package com.wepay.waltz.tools.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.InternalRpcClient;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.zktools.clustermgr.Endpoint;
import io.netty.handler.ssl.SslContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.http.client.fluent.Request;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ServerCli is a tool for interacting with Waltz Server.
 */
public final class ServerCli extends SubcommandCli {

    private ServerCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
            new Subcommand(ServerCli.ListPartition.NAME, ListPartition.DESCRIPTION, ServerCli.ListPartition::new),
            new Subcommand(AddPreferredPartition.NAME, AddPreferredPartition.DESCRIPTION,
                ServerCli.AddPreferredPartition::new),
            new Subcommand(RemovePreferredPartition.NAME, RemovePreferredPartition.DESCRIPTION,
                ServerCli.RemovePreferredPartition::new)
        ));
    }

    /**
     * The {@code ListPartition} command lists:
     * 1. partitions server believes it is the leader for,
     * 2. storage node replicas a server is aware of for each partition.
     */
    private static final class ListPartition extends Cli {
        private static final String PATH = "metrics";
        private static final String SERVER_PARTITION_METRIC_KEY = "waltz-server.waltz-server-num-partitions";
        private static final String SERVER_REPLICA_METRIC_KEY = "waltz-server.replica-info";
        private final ObjectMapper mapper = new ObjectMapper();

        protected static final String NAME = "list";
        protected static final String DESCRIPTION = "List Waltz server partition information";

        protected ListPartition(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option serverOption = Option.builder("s")
                    .longOpt("server")
                    .desc("Specify server in format of host:port, where port is the jetty port")
                    .hasArg()
                    .build();
            serverOption.setRequired(true);
            options.addOption(serverOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String hostAndPort = cmd.getOptionValue("server");

            try {
                String[] hostAndPortArray = hostAndPort.split(":");
                if (hostAndPortArray.length != 2) {
                    throw new IllegalArgumentException("Http must be in format of host:port");
                }
                String host = hostAndPortArray[0];
                int jettyPort = Integer.parseInt(hostAndPortArray[1]);

                String metricsJson = executeGet(host, jettyPort, PATH);
                JsonNode metricsNode = mapper.readTree(metricsJson).path("gauges");
                Integer numPartitions = 0;
                Map<Integer, List<String>> replicaInfo = new HashMap<>();

                if (metricsNode.path(SERVER_PARTITION_METRIC_KEY) != null) {
                    // retrieve server partition info
                    numPartitions = metricsNode.path(SERVER_PARTITION_METRIC_KEY).path("value").asInt();
                }

                if (metricsNode.path(SERVER_REPLICA_METRIC_KEY) != null) {
                    // retrieve replica info
                    String connectStringsJson = metricsNode.path(SERVER_REPLICA_METRIC_KEY).path("value").toString();
                    replicaInfo = mapper.readValue(connectStringsJson, new TypeReference<Map<Integer, List<String>>>() {
                    });
                }

                printResult(numPartitions, replicaInfo);
            } catch (Exception e) {
                throw new SubCommandFailedException(e.getMessage());
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }

        private String executeGet(String host, int port, String path) throws Exception {
            String url = "http://" + host + ":" + port + "/" + path;
            return executeGet(url);
        }

        private String executeGet(String url) throws Exception {
            return Request.Get(url).execute().returnContent().asString();
        }

        /**
         * List all partitions of a server, as well as storage node replicas
         * a server is aware of for each partition.
         * @param numPartitions number of partitions
         * @param replicaInfo   a dict of <partition_id, list<storage_node>>
         */
        private void printResult(Integer numPartitions, Map<Integer, List<String>> replicaInfo) {
            // display partition info
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%nThere are %d partitions for current server%n", numPartitions));

            // display replica info
            for (Map.Entry<Integer, List<String>> entry: replicaInfo.entrySet()) {
                int partitionId = entry.getKey();
                List<String> storageNode = entry.getValue();
                sb.append(String.format("Storage node for partition %d: %s%n", partitionId, storageNode));
            }
            System.out.println(sb.toString());
        }
    }

    /**
     * The {@code AddPreferredPartition} command adds the given preferred partition to the given server.
     */
    private static final class AddPreferredPartition extends Cli {
        protected static final String NAME = "add-preferred-partition";
        protected static final String DESCRIPTION = "add a preferred partition to the server.";

        protected AddPreferredPartition(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option serverOption = Option.builder("s")
                .longOpt("server")
                .desc("Specify server in format of host:port, where port is the server port")
                .hasArg()
                .build();
            Option partitionOption = Option.builder("p")
                .longOpt("partition")
                .desc("Specify the preferred partition to be added to the given server")
                .hasArg()
                .build();
            Option cliCfgOption = Option.builder("c")
                .longOpt("cli-config-path")
                .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path and SSL config")
                .hasArg()
                .build();
            serverOption.setRequired(true);
            partitionOption.setRequired(true);
            cliCfgOption.setRequired(true);

            options.addOption(serverOption);
            options.addOption(partitionOption);
            options.addOption(cliCfgOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String hostAndPort = cmd.getOptionValue("server");
            String partitionId = cmd.getOptionValue("partition");
            String cliConfigPath = cmd.getOptionValue("cli-config-path");

            try {
                String[] hostAndPortArray = hostAndPort.split(":");
                if (hostAndPortArray.length != 2) {
                    throw new IllegalArgumentException("Http must be in format of host:port");
                }
                String host = hostAndPortArray[0];
                int serverPort = Integer.parseInt(hostAndPortArray[1]);

                if (!partitionId.matches("^[0-9]+$")) {
                    throw new IllegalArgumentException(String.format("Partition id '%s' is invalid. Expected a non-negative integer", partitionId));
                }
                addPreferredPartition(host, serverPort, Integer.parseInt(partitionId), cliConfigPath);
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to add preferred partition .%n%s", e));
            }
        }

        private void addPreferredPartition(String host, int serverPort, int partitionId, String cliConfigPath) {
            Endpoint serverEndpoint = new Endpoint(host, serverPort);
            InternalRpcClient rpcClient = null;
            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);
                DummyTxnCallbacks callbacks = new DummyTxnCallbacks();
                rpcClient = new InternalRpcClient(sslContext, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS,
                    callbacks);

                if (!rpcClient.addPreferredPartition(serverEndpoint, partitionId).get()) {
                    System.out.println("Failed to add preferred partition " + partitionId + " to server Endpoint "
                            + serverEndpoint + ".");
                }
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to add preferred partition %s to the "
                    + "server Endpoint %s. %n%s ", partitionId, serverEndpoint.toString(), e));
            } finally {
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
     * The {@code RemovePreferredPartition} command removes the given preferred partition from the given server.
     */
    private static final class RemovePreferredPartition extends Cli {
        protected static final String NAME = "remove-preferred-partition";
        protected static final String DESCRIPTION = "Remove a preferred partition from the server.";

        protected RemovePreferredPartition(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option serverOption = Option.builder("s")
                .longOpt("server")
                .desc("Specify server in format of host:port, where port is the server port")
                .hasArg()
                .build();
            Option partitionOption = Option.builder("p")
                .longOpt("partition")
                .desc("Specify the preferred partition to be removed from the given server")
                .hasArg()
                .build();
            Option cliCfgOption = Option.builder("c")
                .longOpt("cli-config-path")
                .desc("Specify the cli config file path required for ZooKeeper connection string, ZooKeeper root path and SSL config")
                .hasArg()
                .build();
            serverOption.setRequired(true);
            partitionOption.setRequired(true);
            cliCfgOption.setRequired(true);

            options.addOption(serverOption);
            options.addOption(partitionOption);
            options.addOption(cliCfgOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String hostAndPort = cmd.getOptionValue("server");
            String partitionId = cmd.getOptionValue("partition");
            String cliConfigPath = cmd.getOptionValue("cli-config-path");

            try {
                String[] hostAndPortArray = hostAndPort.split(":");
                if (hostAndPortArray.length != 2) {
                    throw new IllegalArgumentException("Http must be in format of host:port");
                }
                String host = hostAndPortArray[0];
                int serverPort = Integer.parseInt(hostAndPortArray[1]);

                if (!partitionId.matches("^[0-9]+$")) {
                    throw new IllegalArgumentException(String.format("Partition id '%s' is invalid. Expected a non-negative integer", partitionId));
                }
                removePreferredPartition(host, serverPort, Integer.parseInt(partitionId), cliConfigPath);
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to remove preferred partition .%n%s", e));
            }
        }

        private void removePreferredPartition(String host, int serverPort, int partitionId, String cliConfigPath) {
            Endpoint serverEndpoint = new Endpoint(host, serverPort);
            InternalRpcClient rpcClient = null;
            try {
                SslContext sslContext = Utils.getSslContext(cliConfigPath, CliConfig.SSL_CONFIG_PREFIX);
                DummyTxnCallbacks callbacks = new DummyTxnCallbacks();
                rpcClient = new InternalRpcClient(sslContext, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS,
                    callbacks);

                if (!rpcClient.removePreferredPartition(serverEndpoint, partitionId).get()) {
                    System.out.println("Failed to remove preferred partition " + partitionId + " from server Endpoint "
                        + serverEndpoint + ".");
                }
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to remove preferred partition %s to the "
                    + "server Endpoint %s. %n%s ", partitionId, serverEndpoint.toString(), e));
            } finally {
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
        new ServerCli(args, true).processCmd();
    }

    public static void main(String[] args) {
        new ServerCli(args, false).processCmd();
    }
}
