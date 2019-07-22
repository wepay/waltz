package com.wepay.waltz.server;

import com.wepay.riff.config.ConfigException;
import com.wepay.riff.config.validator.UniqueValidator;
import com.wepay.riff.config.validator.Validator;
import com.wepay.riff.metrics.graphite.GraphiteReporterConfig;
import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;

public class WaltzServerConfig extends AbstractConfig {

    // ZooKeeper
    public static final String ZOOKEEPER_CONNECT_STRING = "zookeeper.connectString";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.sessionTimeout";

    // Cluster
    public static final String CLUSTER_ROOT = "cluster.root";

    // Server
    public static final String SERVER_PORT = "server.port";
    public static final String SERVER_REGION = "server.region";

    public static final String SERVER_SSL_CONFIG_PREFIX = "server.ssl.";
    public static final String GRAPHITE_REPORTER_CONFIG_PREFIX = "server.graphite.";

    // Partition
    public static final String OPTIMISTIC_LOCK_TABLE_SIZE = "server.optimisticLockTableSize";
    public static final int DEFAULT_OPTIMISTIC_LOCK_TABLE_SIZE = 30000;

    public static final String FEED_CACHE_SIZE = "server.feedCacheSize";
    public static final int DEFAULT_FEED_CACHE_SIZE = 67108864; // 64MB

    public static final String MIN_FETCH_SIZE = "server.minFetchSize";
    public static final int DEFAULT_MIN_FETCH_SIZE = 100;

    public static final String REALTIME_THRESHOLD = "server.realtimeThreshold";
    public static final int DEFAULT_REALTIME_THRESHOLD = 1000;

    public static final String TRANSACTION_DATA_CACHE_SIZE = "server.transactionDataCacheSize";
    public static final int DEFAULT_TRANSACTION_DATA_CACHE_SIZE = 134217728; // 128MB

    public static final String TRANSACTION_DATA_CACHE_ALLOCATION = "server.transactionDataCacheAllocation";
    public static final String DEFAULT_TRANSACTION_DATA_CACHE_ALLOCATION = "heap"; // heap or direct

    // Storage
    public static final String INITIAL_RETRY_INTERVAL = "storage.initialRetryInterval";
    public static final long DEFAULT_INITIAL_RETRY_INTERVAL = 100;

    public static final String MAX_RETRY_INTERVAL = "storage.maxRetryInterval";
    public static final long DEFAULT_MAX_RETRY_INTERVAL = 20000;

    public static final String CHECKPOINT_INTERVAL = "storage.checkpointInterval";
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 250000;

    // Jetty
    public static final String SERVER_JETTY_PORT = "server.jetty.port";

    public WaltzServerConfig(Map<Object, Object> configValues) {
        this("", configValues);
    }

    public WaltzServerConfig(String configPrefix, Map<Object, Object> configValues) {
        super(configPrefix, configValues, new HashMap<String, Parser>() {{
            UniqueValidator portValidator = new UniqueValidator();

            // ZooKeeper
            put(ZOOKEEPER_CONNECT_STRING, stringParser);
            put(ZOOKEEPER_SESSION_TIMEOUT, intParser);

            // Cluster
            put(CLUSTER_ROOT, stringParser);

            // Server
            put(SERVER_PORT, intParser.withValidator(portValidator));
            put(SERVER_REGION, stringParser);

            // See SSLConfig for SSL config parameters
            // See GraphiteReporterConfig for Graphite config parameters

            // Partition
            put(OPTIMISTIC_LOCK_TABLE_SIZE, intParser.withDefault(DEFAULT_OPTIMISTIC_LOCK_TABLE_SIZE));
            put(FEED_CACHE_SIZE, intParser.withDefault(DEFAULT_FEED_CACHE_SIZE));
            put(MIN_FETCH_SIZE, intParser.withDefault(DEFAULT_MIN_FETCH_SIZE));
            put(REALTIME_THRESHOLD, intParser.withDefault(DEFAULT_REALTIME_THRESHOLD));
            put(TRANSACTION_DATA_CACHE_SIZE, intParser.withDefault(DEFAULT_TRANSACTION_DATA_CACHE_SIZE));
            put(TRANSACTION_DATA_CACHE_ALLOCATION, stringParser.withDefault(DEFAULT_TRANSACTION_DATA_CACHE_ALLOCATION)
                .withValidator(new CacheAllocationValidator()));

            // Storage
            put(INITIAL_RETRY_INTERVAL, longParser.withDefault(DEFAULT_INITIAL_RETRY_INTERVAL));
            put(MAX_RETRY_INTERVAL, longParser.withDefault(DEFAULT_MAX_RETRY_INTERVAL));
            put(CHECKPOINT_INTERVAL, intParser.withDefault(DEFAULT_CHECKPOINT_INTERVAL));

            // Jetty
            put(SERVER_JETTY_PORT, intParser.withValidator(portValidator));
        }});
    }

    public SSLConfig getSSLConfig() {
        return new SSLConfig(configPrefix + SERVER_SSL_CONFIG_PREFIX, configValues);
    }

    public GraphiteReporterConfig getGraphiteReporterConfig() {
        return new GraphiteReporterConfig(configPrefix + GRAPHITE_REPORTER_CONFIG_PREFIX, configValues);
    }

    private static class CacheAllocationValidator implements Validator {
        public void validate(String key, Object value) throws ConfigException {
            if (value instanceof String) {
                String v = ((String) value).toLowerCase();
                if (v.equals("heap") || v.equals("direct")) {
                    return;
                }
            }

            throw new ConfigException(
                String.format("Validation failed for %s: unsupported value %s", key, value)
            );
        }
    }

}
