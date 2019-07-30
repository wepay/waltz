package com.wepay.waltz.client;

import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;

public class WaltzClientConfig extends AbstractConfig {

    // ZooKeeper
    public static final String ZOOKEEPER_CONNECT_STRING = "zookeeper.connectString";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.sessionTimeout";

    // Cluster
    public static final String CLUSTER_ROOT = "cluster.root";

    // Client
    public static final String AUTO_MOUNT = "client.autoMount";
    public static final boolean DEFAULT_AUTO_MOUNT = true;

    public static final String NUM_TRANSACTION_RETRY_THREADS = "client.numTransactionRetryThreads";
    public static final int DEFAULT_NUM_TRANSACTION_RETRY_THREADS = 1;

    public static final String NUM_CONSUMER_THREADS = "client.numConsumerThreads";
    public static final int DEFAULT_NUM_CONSUMER_THREADS = 10;

    public static final String LONG_WAIT_THRESHOLD = "client.longWaitThreshold";
    public static final long DEFAULT_LONG_WAIT_THRESHOLD = 5000;

    public static final String CLIENT_SSL_CONFIG_PREFIX = "client.ssl.";

    public static final String MAX_CONCURRENT_TRANSACTIONS = "client.maxConcurrentTransactions";
    public static final int DEFAULT_MAX_CONCURRENT_TRANSACTIONS = 5;

    // Mock (for test only)
    public static final String MOCK_DRIVER = "client.mockDriver";

    private static final HashMap<String, Parser> parsers = new HashMap<>();
    static {
        // ZooKeeper
        parsers.put(ZOOKEEPER_CONNECT_STRING, stringParser);
        parsers.put(ZOOKEEPER_SESSION_TIMEOUT, intParser);

        // Cluster
        parsers.put(CLUSTER_ROOT, stringParser);

        // Client
        parsers.put(AUTO_MOUNT, booleanParser.withDefault(DEFAULT_AUTO_MOUNT));
        parsers.put(NUM_TRANSACTION_RETRY_THREADS, intParser.withDefault(DEFAULT_NUM_TRANSACTION_RETRY_THREADS));
        parsers.put(NUM_CONSUMER_THREADS, intParser.withDefault(DEFAULT_NUM_CONSUMER_THREADS));
        parsers.put(LONG_WAIT_THRESHOLD, longParser.withDefault(DEFAULT_LONG_WAIT_THRESHOLD));
        parsers.put(MAX_CONCURRENT_TRANSACTIONS, intParser.withDefault(DEFAULT_MAX_CONCURRENT_TRANSACTIONS));

        // See SSLConfig for SSL config parameters
    }

    public WaltzClientConfig(Map<Object, Object> configValues) {
        this("", configValues);
    }

    public WaltzClientConfig(String configPrefix, Map<Object, Object> configValues) {
        super(configPrefix, configValues, parsers);
    }

    public SSLConfig getSSLConfig() {
        return new SSLConfig(configPrefix + CLIENT_SSL_CONFIG_PREFIX, configValues);
    }
}
