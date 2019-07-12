package com.wepay.waltz.storage;

import com.wepay.riff.config.validator.UniqueValidator;
import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;

public class WaltzStorageConfig extends AbstractConfig {

    // Storage
    public static final String STORAGE_PORT = "storage.port";
    public static final String STORAGE_ADMIN_PORT = "storage.admin.port";

    public static final String STORAGE_DIRECTORY = "storage.directory";
    public static final String SEGMENT_SIZE_THRESHOLD = "storage.segment.size.threshold";
    public static final long DEFAULT_SEGMENT_SIZE_THRESHOLD = 1000000000L;

    public static final String STORAGE_SSL_CONFIG_PREFIX = "storage.ssl.";

    // Jetty
    public static final String STORAGE_JETTY_PORT = "storage.jetty.port";

    public static final String CLUSTER_NUM_PARTITIONS = "cluster.numPartitions";
    public static final String CLUSTER_KEY = "cluster.key";

    public static final String STORAGE_SEGMENT_CACHE_CAPACITY = "storage.segment.cache.capacity";
    public static final int DEFAULT_STORAGE_SEGMENT_CACHE_CAPACITY = 5;

    public WaltzStorageConfig(Map<Object, Object> configValues) {
        this("", configValues);
    }

    public WaltzStorageConfig(String configPrefix, Map<Object, Object> configValues) {
        super(configPrefix, configValues, new HashMap<String, Parser>() {{
            UniqueValidator portValidator = new UniqueValidator();

            put(STORAGE_PORT, intParser.withValidator(portValidator));
            put(STORAGE_ADMIN_PORT, intParser.withValidator(portValidator));
            put(STORAGE_DIRECTORY, stringParser);
            put(SEGMENT_SIZE_THRESHOLD, longParser.withDefault(DEFAULT_SEGMENT_SIZE_THRESHOLD));

            // See SSLConfig for SSL config parameters

            // Jetty
            put(STORAGE_JETTY_PORT, intParser.withValidator(portValidator));

            put(CLUSTER_NUM_PARTITIONS, intParser);
            put(CLUSTER_KEY, uuidParser);

            put(STORAGE_SEGMENT_CACHE_CAPACITY, intParser.withDefault(DEFAULT_STORAGE_SEGMENT_CACHE_CAPACITY));
        }});
    }

    public SSLConfig getSSLConfig() {
        return new SSLConfig(configPrefix + STORAGE_SSL_CONFIG_PREFIX, configValues);
    }

}
