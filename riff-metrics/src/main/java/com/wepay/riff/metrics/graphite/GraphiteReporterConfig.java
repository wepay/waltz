package com.wepay.riff.metrics.graphite;

import com.wepay.riff.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Config class for GraphiteReporter.
 */
public class GraphiteReporterConfig extends AbstractConfig {
    public static final String HOSTNAME = "hostname";

    public static final String PORT = "port";
    public static final int DEFAULT_PORT = 2003;

    public static final String PREFIX = "prefix";
    public static final String DEFAULT_PREFIX = "waltz";

    public static final String REPORT_INTERVAL_SECONDS = "reportIntervalMs";
    public static final int DEFAULT_REPORT_INTERVAL_SECONDS = 60;

    public static final String USE_UDP = "useUdp";
    public static final Boolean DEFAULT_USE_UDP = false;

    private static final HashMap<String, Parser> parsers = new HashMap<>();
    static {
        parsers.put(HOSTNAME, stringParser);
        parsers.put(PORT, intParser.withDefault(DEFAULT_PORT));
        parsers.put(PREFIX, stringParser.withDefault(DEFAULT_PREFIX));
        parsers.put(REPORT_INTERVAL_SECONDS, intParser.withDefault(DEFAULT_REPORT_INTERVAL_SECONDS));
        parsers.put(USE_UDP, booleanParser.withDefault(DEFAULT_USE_UDP));
    }


    public GraphiteReporterConfig(String configPrefix, Map<Object, Object> configValues) {
        super(configPrefix, configValues, parsers);
    }
}
