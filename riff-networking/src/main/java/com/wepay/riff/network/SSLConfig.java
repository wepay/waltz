package com.wepay.riff.network;

import com.wepay.riff.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;

public class SSLConfig extends AbstractConfig {

    public static final String KEY_STORE_LOCATION = "keyStore.location";
    public static final String KEY_STORE_PASSWORD = "keyStore.password";
    public static final String KEY_STORE_TYPE = "keyStore.type";
    public static final String DEFAULT_KEY_STORE_TYPE = "JKS";

    public static final String TRUST_STORE_LOCATION = "trustStore.location";
    public static final String TRUST_STORE_PASSWORD = "trustStore.password";
    public static final String TRUST_STORE_TYPE = "trustStore.type";
    public static final String DEFAULT_TRUST_STORE_TYPE = "JKS";

    public static final String KEY_MANAGER_ALGORITHM = "keyManager.algorithm";
    public static final String DEFAULT_KEY_MANAGER_ALGORITHM = "SunX509";

    public static final String TRUST_MANAGER_ALGORITHM = "trustManager.algorithm";
    public static final String DEFAULT_TRUST_MANAGER_ALGORITHM = "SunX509";

    private static final HashMap<String, Parser> parsers = new HashMap<>();
    static {
        parsers.put(KEY_STORE_LOCATION, stringParser);
        parsers.put(KEY_STORE_PASSWORD, stringParser);
        parsers.put(KEY_STORE_TYPE, stringParser.withDefault(SSLConfig.DEFAULT_KEY_STORE_TYPE));
        parsers.put(TRUST_STORE_LOCATION, stringParser);
        parsers.put(TRUST_STORE_PASSWORD, stringParser);
        parsers.put(TRUST_STORE_TYPE, stringParser.withDefault(SSLConfig.DEFAULT_TRUST_STORE_TYPE));
        parsers.put(KEY_MANAGER_ALGORITHM, stringParser.withDefault(SSLConfig.DEFAULT_KEY_MANAGER_ALGORITHM));
        parsers.put(TRUST_MANAGER_ALGORITHM, stringParser.withDefault(SSLConfig.DEFAULT_TRUST_MANAGER_ALGORITHM));
    }

    public SSLConfig(String configPrefix, Map<Object, Object> configValues) {
        super(configPrefix, configValues, parsers);
    }

}
