package com.wepay.waltz.tools;

import com.wepay.riff.config.AbstractConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used by the Cli to configure/parse the cli_config file.
 */
public class CliConfig extends AbstractConfig {
    // Storage
    public static final String SSL_CONFIG_PREFIX = "ssl.";

    // ZooKeeper
    public static final String ZOOKEEPER_CONNECT_STRING = "zookeeper.connectString";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.sessionTimeout";
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 30000;

    // Cluster
    public static final String CLUSTER_ROOT = "cluster.root";

    private static final HashMap<String, Parser> parsers = new HashMap<>();
    static {
        // ZooKeeper
        parsers.put(ZOOKEEPER_CONNECT_STRING, stringParser);
        parsers.put(ZOOKEEPER_SESSION_TIMEOUT, intParser.withDefault(DEFAULT_ZOOKEEPER_SESSION_TIMEOUT));

        // Cluster
        parsers.put(CLUSTER_ROOT, stringParser);
    }

    public CliConfig(Map<Object, Object> configValues) {
        this("", configValues);
    }

    public CliConfig(String configPrefix, Map<Object, Object> configValues) {
        super(configPrefix, configValues, parsers);
    }

    public static CliConfig parseCliConfigFile(String cliConfigPath) throws IOException {
        Yaml yaml = new Yaml();

        try (FileInputStream in = new FileInputStream(cliConfigPath)) {
            Map<Object, Object> props = yaml.load(in);
            CliConfig cliConfig = new CliConfig(props);

            return cliConfig;
        }
    }
}
