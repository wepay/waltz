package com.wepay.waltz.client;

import com.wepay.riff.config.ConfigException;
import com.wepay.riff.config.validator.Validator;
import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the Waltz Client.
 */
public class WaltzClientConfig extends AbstractConfig {

    /** Zookeeper connection information, <code>zookeeper.connectString</code>. */
    public static final String ZOOKEEPER_CONNECT_STRING = "zookeeper.connectString";

    /** Zookeeper session timeout, <code>zookeeper.sessionTimeout</code>. */
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.sessionTimeout";

    /** Waltz cluster root, <code>cluster.root</code>. */
    public static final String CLUSTER_ROOT = "cluster.root";

    /** Determines whether to mount all the partitions while creating WaltzClient, <code>client.autoMount</code>. */
    public static final String AUTO_MOUNT = "client.autoMount";
    /** Default value for {@link #AUTO_MOUNT} config. */
    public static final boolean DEFAULT_AUTO_MOUNT = true;

    /** Number of transaction retry threads, <code>client.numTransactionRetryThreads</code>. */
    public static final String NUM_TRANSACTION_RETRY_THREADS = "client.numTransactionRetryThreads";
    /** Default value for {@link #NUM_TRANSACTION_RETRY_THREADS} config. */
    public static final int DEFAULT_NUM_TRANSACTION_RETRY_THREADS = 1;

    /** Number of consumer threads for processing committed transactions, <code>client.numConsumerThreads</code>. */
    public static final String NUM_CONSUMER_THREADS = "client.numConsumerThreads";
    /** Default value for {@link #NUM_CONSUMER_THREADS} config. */
    public static final int DEFAULT_NUM_CONSUMER_THREADS = 10;

    /** Threshold wait time in millis before nudging waiting transactions, <code>client.longWaitThreshold</code>. */
    public static final String LONG_WAIT_THRESHOLD = "client.longWaitThreshold";
    /** Default value for {@link #LONG_WAIT_THRESHOLD} config. */
    public static final long DEFAULT_LONG_WAIT_THRESHOLD = 5000;

    /** Client SSL Config prefix, <code>client.ssl.</code>. */
    public static final String CLIENT_SSL_CONFIG_PREFIX = "client.ssl.";

    /** Maximum number of concurrent transactions per partition, <code>client.maxConcurrentTransactions</code>. */
    public static final String MAX_CONCURRENT_TRANSACTIONS = "client.maxConcurrentTransactions";
    /** Default value for {@link #MAX_CONCURRENT_TRANSACTIONS} config. */
    public static final int DEFAULT_MAX_CONCURRENT_TRANSACTIONS = 5;

    /** Mock driver for test only, <code>client.mockDriver</code>. */
    public static final String MOCK_DRIVER = "client.mockDriver";
    /** Default value for {@link #MOCK_DRIVER} config. */
    public static final Object DEFAULT_MOCK_DRIVER = null;

    private static final Validator mockDriverValidator = (key, value) -> {
        if (!key.equals(MOCK_DRIVER)) {
            throw new ConfigException("Expecting " + MOCK_DRIVER + " key. Instead got " + key);
        }

        if (value != null && !isJUnitTest()) {
            throw new ConfigException("Mock driver is for test only");
        }
    };

    private static final Parser mockDriverParser = new Parser(null);

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
        parsers.put(MOCK_DRIVER, mockDriverParser.withDefault(DEFAULT_MOCK_DRIVER).withValidator(mockDriverValidator));

        // See SSLConfig for SSL config parameters
    }

    /**
     * Class Constructor, with config prefix as <code>""</code>.
     *
     * @param configValues the config values.
     */
    public WaltzClientConfig(Map<Object, Object> configValues) {
        this("", configValues);
    }

    /**
     * Class Constructor, with config prefix as <code>configPrefix</code>.
     *
     * @param configPrefix the config prefix.
     * @param configValues the config values.
     */
    public WaltzClientConfig(String configPrefix, Map<Object, Object> configValues) {
        super(configPrefix, configValues, parsers);
    }

    /**
     * @return SSLConfig.
     */
    public SSLConfig getSSLConfig() {
        return new SSLConfig(configPrefix + CLIENT_SSL_CONFIG_PREFIX, configValues);
    }

    /**
     * Checks if code is running from a unit test
     * @return true if yes, false otherwise
     */
    private static boolean isJUnitTest() {
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("org.junit.")) {
                return true;
            }
        }
        return false;
    }
}
