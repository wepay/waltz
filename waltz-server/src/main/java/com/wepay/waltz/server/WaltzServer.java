package com.wepay.waltz.server;

import com.wepay.riff.config.ConfigException;
import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.metrics.core.MetricGroup;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.graphite.GraphiteReporter;
import com.wepay.riff.metrics.graphite.GraphiteReporterConfig;
import com.wepay.riff.metrics.health.HealthCheckRegistry;
import com.wepay.riff.metrics.jmx.JmxReporter;
import com.wepay.riff.metrics.servlets.BuildInfoServlet;
import com.wepay.riff.metrics.servlets.HealthCheckServlet;
import com.wepay.riff.metrics.servlets.MetricsServlet;
import com.wepay.riff.metrics.servlets.PingServlet;
import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.NetworkServer;
import com.wepay.riff.network.ServerSSL;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.ServerException;
import com.wepay.waltz.server.health.HealthCheck;
import com.wepay.waltz.server.internal.FeedCache;
import com.wepay.waltz.server.internal.FeedCachePartition;
import com.wepay.waltz.server.internal.Partition;
import com.wepay.waltz.server.internal.TransactionFetcher;
import com.wepay.waltz.server.internal.WaltzServerCli;
import com.wepay.waltz.server.internal.WaltzServerHandler;
import com.wepay.waltz.store.Store;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.store.internal.StoreImpl;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.common.metadata.StoreParams;
import com.wepay.waltz.common.metadata.StoreParamsSerializer;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.DynamicPartitionAssignmentPolicy;
import com.wepay.zktools.util.State;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import io.netty.handler.ssl.SslContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Main implementation of the Waltz Server API.
 */
public class WaltzServer {

    private static final Logger logger = Logging.getLogger(WaltzServer.class);
    private static final MetricRegistry REGISTRY = MetricRegistry.getInstance();

    public final int port;

    private final Object lock = new Object();
    protected final WaltzServerConfig config;
    protected final Store store;
    private final State<Boolean> running = new State<>(true);
    private final ManagedServer managedServer;
    private final ClusterManager clusterManager;
    private final NetworkServer networkServer;
    private final Endpoint endpoint;
    private final Map<Integer, Partition> partitions;
    private final String metricsGroup = MetricGroup.WALTZ_SERVER_METRIC_GROUP;
    private String clusterName = null;
    private int serverId;
    private Server jettyServer;
    protected final FeedCache feedCache;
    protected final TransactionFetcher transactionFetcher;

    /**
     * Class constructor.
     * @param port Server port.
     * @param sslCtx The {@link javax.net.ssl.SSLContext} used to instantiate the Waltz Server.
     * @param store The {@link Store} associated with the Waltz Server.
     * @param clusterManager {@link ClusterManager} instance.
     * @param config The Waltz Server configuration file.
     * @throws Exception thrown in case the Waltz Server instance cannot be created.
     */
    public WaltzServer(final int port, SslContext sslCtx, Store store, ClusterManager clusterManager,
                       WaltzServerConfig config) throws Exception {
        logVersion();
        checkConfig(config);

        this.port = port;
        this.config = config;
        this.partitions = new HashMap<>();
        this.networkServer = new NetworkServer(port, sslCtx != null ? sslCtx : ServerSSL.createInsecureContext()) {
            @Override
            protected MessageHandler getMessageHandler() {
                return new WaltzServerHandler(partitions, store);
            }
        };

        try {
            endpoint = new Endpoint(InetAddress.getLocalHost().getCanonicalHostName(), port);
        } catch (UnknownHostException ex) {
            logger.error("failed to create endpoint", ex);
            throw ex;
        }

        this.store = store;

        Meter feedCacheMissMeter = REGISTRY.meter(metricsGroup, "feed-cache-miss");

        this.feedCache = new FeedCache((int) config.get(WaltzServerConfig.FEED_CACHE_SIZE), feedCacheMissMeter);

        Meter transactionCacheMissMeter = REGISTRY.meter(metricsGroup, "transaction-cache-miss");

        this.transactionFetcher = new TransactionFetcher(
            (int) config.get(WaltzServerConfig.TRANSACTION_DATA_CACHE_SIZE),
            ((String) config.get(WaltzServerConfig.TRANSACTION_DATA_CACHE_ALLOCATION)).toLowerCase().equals("direct"),
            transactionCacheMissMeter
        );

        // Create an interface object for the cluster manager
        this.managedServer = new ManagedServer() {

            @Override
            public void setClusterName(String clusterName) {
                WaltzServer.this.clusterName = clusterName;
            }

            @Override
            public void setServerId(int serverId) {
                WaltzServer.this.serverId = serverId;
            }

            @Override
            public Endpoint endpoint() throws ClusterManagerException {
                return endpoint;
            }

            @Override
            public void setPartitions(List<PartitionInfo> partitionInfos) {
                synchronized (partitions) {
                    HashSet<Integer> remaining = new HashSet<>(partitions.keySet());

                    for (PartitionInfo info : partitionInfos) {
                        Partition partition = partitions.get(info.partitionId);
                        if (partition == null) {
                            // Add a new partition
                            partition = createPartition(info);
                            try {
                                partition.open();
                            } catch (StoreException ex) {
                                partition.closeAsync();
                            }
                            partitions.put(info.partitionId, partition);
                        } else {
                            // Update the generation
                            partition.generation(info.generation);
                        }
                        remaining.remove(info.partitionId);
                    }
                    // Remove extraneous partitions
                    for (Integer partitionId : remaining) {
                        Partition partition = partitions.remove(partitionId);
                        if (partition != null) {
                            partition.closeAsync();
                        }
                    }
                    logger.info("partition assigned: " + getPartitionIds() + " endpoint=" + endpoint);
                }
            }
        };

        this.clusterManager = clusterManager;
        clusterManager.manage(managedServer);

        registerMetrics();

        logger.info("WaltzServer Started: id=" + serverId + " endpoint=" + endpoint);
    }

    protected Partition createPartition(PartitionInfo info) {
        StorePartition storePartition = store.getPartition(info.partitionId, info.generation);
        FeedCachePartition feedCachePartition = feedCache.getPartition(info.partitionId);
        return new Partition(info.partitionId, storePartition, feedCachePartition, transactionFetcher, config);
    }

    Map<Integer, Partition> partitions() {
        return Collections.unmodifiableMap(partitions);
    }

    /**
     * Closes the Waltz Server instance.
     */
    public void close() {
        logger.info("Closing WaltzServer: id=" + serverId + " endpoint=" + endpoint);

        try {
            clusterManager.unmanage(managedServer);

        } catch (ClusterManagerException ex) {
            logger.error("failed to unmanage the Waltz server", ex);
        }

        try {
            networkServer.close();

        } catch (Throwable ex) {
            logger.error("failed to close the network server", ex);
        }

        synchronized (partitions) {
            for (Partition partition : partitions.values()) {
                try {
                    partition.close();
                } catch (Throwable ex) {
                    logger.error("failed to close a partition: partitionId=" + partition.partitionId, ex);
                }
            }
            partitions.clear();
        }

        try {
            store.close();

        } catch (Throwable ex) {
            logger.error("failed to close the store", ex);
        }

        try {
            synchronized (lock) {
                if (jettyServer != null) {
                    jettyServer.stop();
                }
            }
        } catch (Throwable ex) {
            logger.error("failed to close jetty server", ex);
        }

        try {
            feedCache.close();

        } catch (Throwable ex) {
            logger.error("failed to close feed cache", ex);
        }

        try {
            transactionFetcher.close();

        } catch (Throwable ex) {
            logger.error("failed to close transaction data fetcher", ex);
        }

        unregisterMetrics();

        running.set(false);

        logger.info("WaltzServer Closed: id=" + serverId + " endpoint=" + endpoint);
    }

    /**
     * Starts the jetty server by using the jetty port to configure the Waltz server metrics.
     * @param zkClient The ZooKeeper client used for the Waltz cluster.
     * @throws Exception thrown if the jetty server is already started.
     */
    public void setJettyServer(ZooKeeperClient zkClient) throws Exception {
        Optional<Object> jettyPortOpt = config.getOpt(WaltzServerConfig.SERVER_JETTY_PORT);
        if (jettyPortOpt.isPresent()) {
            synchronized (lock) {
                if (jettyServer == null) {
                    logger.info("JettyPort is : " + (int) jettyPortOpt.get());

                    InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), (int) jettyPortOpt.get());
                    jettyServer = new Server(addr);

                    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
                    context.setContextPath("/");
                    jettyServer.setHandler(context);

                    context.addServlet(new ServletHolder(new PingServlet()), "/ping");
                    context.addServlet(new ServletHolder(new BuildInfoServlet(Utils.getBuildInfoMap(this.getClass()))), "/buildinfo");
                    context.addServlet(new ServletHolder(new MetricsServlet(MetricRegistry.getInstance())), "/metrics");

                    HealthCheckRegistry healthCheckRegistry = registerHealthCheck(this, zkClient);
                    context.addServlet(new ServletHolder(new HealthCheckServlet(healthCheckRegistry)), "/health");
                    jettyServer.start();

                } else {
                    IllegalStateException ex = new IllegalStateException("jetty already started");
                    logger.error("jetty already started", ex);
                    throw ex;
                }
            }
        } else {
            logger.warn("Jetty Server failed to start: " + WaltzServerConfig.SERVER_JETTY_PORT + " missing");
        }
    }

    /**
     * Returns the health of the partitions.
     * @return the health of the partitions.
     */
    public Map<Integer, Boolean> getPartitionHealth() {
        Map<Integer, Boolean> partitionHealth = new HashMap<>();

        synchronized (partitions) {
            for (Partition partition : partitions.values()) {
                partitionHealth.put(partition.partitionId, partition.isHealthy());
            }
            return partitionHealth;
        }
    }

    /**
     * Returns True if the Waltz server is not closed, otherwise returns False.
     * @return True if the Waltz server is not closed, otherwise returns False.
     */
    public boolean isClosed() {
        return !running.get();
    }

    /**
     * Returns list of partition IDs of this Waltz server.
     * @return list of partition IDs of this Waltz server.
     */
    public List<Integer> getPartitionIds() {
        ArrayList<Integer> partitionIds;

        synchronized (partitions) {
            partitionIds = new ArrayList<>(partitions.keySet());
        }
        Collections.sort(partitionIds);

        return partitionIds;
    }

    public void closeNetworkServer() {
        try {
            networkServer.close();
        } catch (Throwable ex) {
            logger.error("failed to close the network server", ex);
        }
    }

    /**
     * Starts the Waltz server and jetty server.
     * @param args The CLI arguments.
     * @throws Exception thrown in case the Waltz server cannot be started.
     */
    public static void main(String[] args) throws Exception {
        WaltzServerCli cli = new WaltzServerCli(args);
        cli.processCmd();
        String configPath = cli.getConfigPath();

        Yaml yaml = new Yaml();
        ZooKeeperClient zkClient = null;
        ClusterManager clusterManager = null;

        try (FileInputStream input = new FileInputStream(configPath)) {
            Map<Object, Object> props = yaml.load(input);

            WaltzServerConfig config = new WaltzServerConfig(props);

            String zkConnectString = (String) config.get(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING);
            int zkSessionTimeout = (int) config.get(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT);
            zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout);

            ZNode root = new ZNode((String) config.get(WaltzServerConfig.CLUSTER_ROOT));
            clusterManager = new ClusterManagerImpl(zkClient, root, new DynamicPartitionAssignmentPolicy());

            ZNode storeRoot = new ZNode(root, "store");

            NodeData<StoreParams> storeParams = zkClient.getData(storeRoot, StoreParamsSerializer.INSTANCE);

            if (storeParams.value == null) {
                throw new ServerException("store parameter not specified");
            }

            if (clusterManager.numPartitions() != storeParams.value.numPartitions) {
                throw new ServerException(
                    "inconsistent number of partition: clusterManager=" + clusterManager.numPartitions()
                    + " store=" + storeParams.value.numPartitions
                );
            }

            Store store = new StoreImpl(zkClient, storeRoot, config);

            int port = (int) config.get(WaltzServerConfig.SERVER_PORT);

            SslContext sslCtx = ServerSSL.createContext(config.getSSLConfig());

            JmxReporter.forRegistry(MetricRegistry.getInstance()).build().start();

            GraphiteReporterConfig graphiteReporterConfig = config.getGraphiteReporterConfig();
            Optional<GraphiteReporter> maybeGraphiteReporter = Utils.getGraphiteReporter(MetricRegistry.getInstance(), graphiteReporterConfig);
            maybeGraphiteReporter.ifPresent(graphiteReporter ->
                graphiteReporter.start((Integer) graphiteReporterConfig.get(GraphiteReporterConfig.REPORT_INTERVAL_SECONDS), TimeUnit.SECONDS));

            // Start server
            WaltzServer server = new WaltzServer(port, sslCtx, store, clusterManager, config);

            // Start Jetty server
            server.setJettyServer(zkClient);

            server.running.await(false);

        } catch (IOException e) {
            System.err.print(e + "\n");

        } finally {
            if (clusterManager != null) {
                clusterManager.close();
            }
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    private static HealthCheckRegistry registerHealthCheck(WaltzServer server, ZooKeeperClient zkClient) {
        HealthCheck healthCheck = new HealthCheck(zkClient, server);
        HealthCheckRegistry registry = new HealthCheckRegistry();
        registry.register("server-health-check", new com.wepay.riff.metrics.health.HealthCheck() {
            @Override
            protected Result check() {
                Map<Integer, Boolean> partitionHealth = healthCheck.getPartitionHealth();
                boolean dbHealthy = partitionHealth.values().stream().allMatch(Boolean::booleanValue);
                boolean zkHealthy = healthCheck.zkIsHealthy();
                if (zkHealthy && dbHealthy) {
                    return Result.builder().healthy().withDetail("zookeeper", zkHealthy).withDetail("partitions", partitionHealth).build();
                } else {
                    return Result.builder().unhealthy().withDetail("zookeeper", zkHealthy).withDetail("partitions", partitionHealth).build();
                }
            }
        });
        return registry;
    }

    private void registerMetrics() {
        REGISTRY.gauge(metricsGroup, "is-closed", (Gauge<Boolean>) () -> isClosed());
        REGISTRY.gauge(metricsGroup, "cluster-name", (Gauge<String>) () -> clusterName);
        REGISTRY.gauge(metricsGroup, "server-id", (Gauge<Integer>) () -> serverId);
        REGISTRY.gauge(metricsGroup, "endpoint", (Gauge<String>) () -> endpoint.toString());
        REGISTRY.gauge(metricsGroup, "waltz-server-num-partitions", (Gauge<Integer>) () -> getPartitionIds().size());
        REGISTRY.gauge(metricsGroup, "replica-info", (Gauge<Map<Integer, List<String>>>) () -> getReplicaInfoMap());
    }

    private void unregisterMetrics() {
        REGISTRY.remove(metricsGroup, "transaction-cache-miss");
        REGISTRY.remove(metricsGroup, "is-closed");
        REGISTRY.remove(metricsGroup, "cluster-name");
        REGISTRY.remove(metricsGroup, "server-id");
        REGISTRY.remove(metricsGroup, "endpoint");
        REGISTRY.remove(metricsGroup, "waltz-server-num-partitions");
        REGISTRY.remove(metricsGroup, "replica-info");
    }

    private Map<Integer, List<String>> getReplicaInfoMap() {
        Set<ReplicaId> replicaIds = store.getReplicaIds();
        Map<Integer, List<String>> replicaInfo = new HashMap<>();
        for (ReplicaId replicaId: replicaIds) {
            int partitionId = replicaId.partitionId;
            String connectString = replicaId.storageNodeConnectString;
            List<String> connectStringList = replicaInfo.computeIfAbsent(partitionId, k -> new ArrayList<>());
            connectStringList.add(connectString);
        }
        return replicaInfo;
    }

    private static void logVersion() {
        String title = WaltzServer.class.getPackage().getImplementationTitle();
        String version = WaltzServer.class.getPackage().getImplementationVersion();

        logger.info("STARTING [name=" + title + " version=" + version + "]");
    }

    private static void checkConfig(WaltzServerConfig config) {
        int minFetchSize = (int) config.get(WaltzServerConfig.MIN_FETCH_SIZE);
        int realtimeThreshold = (int) config.get(WaltzServerConfig.REALTIME_THRESHOLD);

        // minFetchSize must be smaller than realtimeThreshold. Otherwise, the catchup feed task may be stalled.
        // The catch-up feed task uses FIFO queue to manage feed contexts. It assumes there is always a transaction to send.
        if (minFetchSize >= realtimeThreshold) {
            throw new ConfigException(
                String.format("\"%s\" must be smaller than \"%s\"",
                    WaltzServerConfig.MIN_FETCH_SIZE, WaltzServerConfig.REALTIME_THRESHOLD
                )
            );
        }
    }

}
