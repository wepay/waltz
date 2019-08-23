package com.wepay.waltz.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wepay.riff.metrics.core.Gauge;
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
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.server.health.Healthcheck;
import com.wepay.waltz.storage.server.internal.AdminServerHandler;
import com.wepay.waltz.storage.server.internal.PartitionInfoSnapshot;
import com.wepay.waltz.storage.server.internal.StorageManager;
import com.wepay.waltz.storage.server.internal.StorageServerHandler;
import com.wepay.waltz.storage.server.internal.WaltzStorageCli;
import com.wepay.zktools.util.State;
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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class WaltzStorage {

    private static final Logger logger = Logging.getLogger(WaltzStorage.class);
    private static final MetricRegistry REGISTRY = MetricRegistry.getInstance();

    public final int port;
    public final int adminPort;

    protected final WaltzStorageConfig config;
    private final State<Boolean> running = new State<>(true);
    private final NetworkServer networkServer;
    private final NetworkServer adminNetworkServer;
    private final StorageManager storageManager;
    private final String metricsGroup = MetricGroup.WALTZ_STORAGE_METRIC_GROUP;
    private Server jettyServer;

    public WaltzStorage(final int port, final int adminPort, SslContext sslCtx, WaltzStorageConfig config) throws Exception {
        logVersion();

        this.port = port;
        this.adminPort = adminPort;
        this.config = config;
        this.storageManager = new StorageManager(
                (String) config.get(WaltzStorageConfig.STORAGE_DIRECTORY),
                (long) config.get(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD),
                (Integer) config.get(WaltzStorageConfig.CLUSTER_NUM_PARTITIONS),
                (UUID) config.get(WaltzStorageConfig.CLUSTER_KEY),
                (Integer) config.get(WaltzStorageConfig.STORAGE_SEGMENT_CACHE_CAPACITY)
        );
        this.networkServer = new NetworkServer(port, sslCtx != null ? sslCtx : ServerSSL.createInsecureContext()) {
            @Override
            protected MessageHandler getMessageHandler() {
                return new StorageServerHandler(storageManager);
            }
        };
        this.adminNetworkServer = new NetworkServer(adminPort, sslCtx != null ? sslCtx : ServerSSL.createInsecureContext()) {
            @Override
            protected MessageHandler getMessageHandler() {
                return new AdminServerHandler(storageManager);
            }
        };

        String host = InetAddress.getLocalHost().getCanonicalHostName();
        logger.info("WaltzStorage Started: " + host + ":" + port);
        logger.info("WaltzStorage Admin Started: " + host + ":" + adminPort);

        registerMetrics();
    }

    public void close() {
        adminNetworkServer.close();
        networkServer.close();
        storageManager.close();

        try {
            if (jettyServer != null) {
                jettyServer.stop();
            }
        } catch (Throwable ex) {
            logger.error("failed to close jetty server", ex);
        }

        unregisterMetrics();

        running.set(false);
    }

    public void setJettyServer() throws Exception {
        Optional<Object> jettyPortOpt = config.getOpt(WaltzStorageConfig.STORAGE_JETTY_PORT);
        if (jettyPortOpt.isPresent()) {
            logger.info("JettyPort is : " + (int) jettyPortOpt.get());

            InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), (int) jettyPortOpt.get());
            jettyServer = new Server(addr);

            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            jettyServer.setHandler(context);

            context.addServlet(new ServletHolder(new PingServlet()), "/ping");
            context.addServlet(new ServletHolder(new BuildInfoServlet(Utils.getBuildInfoMap(this.getClass()))), "/buildinfo");
            context.addServlet(new ServletHolder(new MetricsServlet(MetricRegistry.getInstance())), "/metrics");

            HealthCheckRegistry healthCheckRegistry = registerHealthCheck(this);
            context.addServlet(new ServletHolder(new HealthCheckServlet(healthCheckRegistry)), "/health");

            jettyServer.start();
        } else {
            logger.warn("Jetty Server failed to start: " + WaltzStorageConfig.STORAGE_JETTY_PORT + " missing");
        }
    }

    public Set<PartitionInfoSnapshot> getPartitionInfos() throws StorageException, IOException  {
        return storageManager.getPartitionInfos();
    }

    public Set<Integer> getAssignedPartitionIds() throws StorageException, IOException  {
        return storageManager.getAssignedPartitionIds();
    }

    public static void main(String[] args) throws Exception {
        WaltzStorageCli cli = new WaltzStorageCli(args);
        cli.processCmd();
        String configPath = cli.getConfigPath();

        try (FileInputStream input = new FileInputStream(configPath)) {
            Yaml yaml = new Yaml();
            Map<Object, Object> props = yaml.load(input);

            WaltzStorageConfig config = new WaltzStorageConfig(props);

            int port = (int) config.get(WaltzStorageConfig.STORAGE_PORT);
            int adminPort = (int) config.get(WaltzStorageConfig.STORAGE_ADMIN_PORT);

            SslContext sslCtx = ServerSSL.createContext(config.getSSLConfig());

            JmxReporter.forRegistry(MetricRegistry.getInstance()).build().start();

            GraphiteReporterConfig graphiteReporterConfig = config.getGraphiteReporterConfig();
            Optional<GraphiteReporter> maybeGraphiteReporter = Utils.getGraphiteReporter(MetricRegistry.getInstance(), graphiteReporterConfig);
            maybeGraphiteReporter.ifPresent(graphiteReporter ->
                graphiteReporter.start((Integer) graphiteReporterConfig.get(GraphiteReporterConfig.REPORT_INTERVAL_SECONDS), TimeUnit.SECONDS));

            // Start storage server
            WaltzStorage server = new WaltzStorage(port, adminPort, sslCtx, config);

            // Start Jetty server
            server.setJettyServer();

            server.running.await(false);
        } catch (IOException e) {
            System.err.print(e + "\n");
        }
    }

    private static void logVersion() {
        String title = WaltzStorage.class.getPackage().getImplementationTitle();
        String version = WaltzStorage.class.getPackage().getImplementationVersion();

        logger.info("STARTING [name=" + title + " version=" + version + "]");
    }

    public static int[] checksums(UUID key, int numPartitions, WaltzStorageConfig config) throws Exception {
        StorageManager storageManager = new StorageManager(
                (String) config.get(WaltzStorageConfig.STORAGE_DIRECTORY),
                (long) config.get(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD),
                (Integer) config.get(WaltzStorageConfig.CLUSTER_NUM_PARTITIONS),
                (UUID) config.get(WaltzStorageConfig.CLUSTER_KEY),
                (Integer) config.get(WaltzStorageConfig.STORAGE_SEGMENT_CACHE_CAPACITY)
        );
        storageManager.open(key, numPartitions);

        int[] checksums = new int[numPartitions];
        try {
            for (int i = 0; i < numPartitions; i++) {
                checksums[i] = storageManager.checksum(i);
            }
        } finally {
            storageManager.close();
        }

        return checksums;
    }

    private static HealthCheckRegistry registerHealthCheck(WaltzStorage storage) {
        Healthcheck healthcheck = new Healthcheck(storage);
        HealthCheckRegistry registry = new HealthCheckRegistry();
        registry.register("storage-health-check", new com.wepay.riff.metrics.health.HealthCheck() {
            @Override
            protected Result check() throws JsonProcessingException {
                String message = healthcheck.getMessage();
                if (healthcheck.isHealthy()) {
                    return Result.healthy(message);
                } else {
                    return Result.unhealthy(message);
                }
            }
        });
        return registry;
    }

    private void registerMetrics() {
        REGISTRY.gauge(metricsGroup, "waltz-storage-num-partitions", (Gauge<Integer>) () -> {
            try {
                return getAssignedPartitionIds().size();
            } catch (Exception e) {
                logger.warn("waltz-storage-num-partitions metric registration failed with an exception: " + e);
                return -1;
            }
        });
        REGISTRY.gauge(metricsGroup, "waltz-storage-partition-ids", (Gauge<Set<Integer>>) () -> {
            try {
                return getAssignedPartitionIds();
            } catch (Exception e) {
                logger.warn("waltz-storage-partition-ids metric registration failed with an exception: " + e);
                return Collections.emptySet();
            }
        });
    }

    private void unregisterMetrics() {
        REGISTRY.remove(metricsGroup, "waltz-storage-num-partitions");
        REGISTRY.remove(metricsGroup, "waltz-storage-partition-ids");
    }
}
