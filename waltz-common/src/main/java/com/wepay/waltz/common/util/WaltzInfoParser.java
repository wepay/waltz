package com.wepay.waltz.common.util;

import com.wepay.riff.util.Logging;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * WaltzInfoParser loads waltz properties from the waltz-version.properties file created a build time.
 */
public class WaltzInfoParser {
    private static final Logger logger = Logging.getLogger(WaltzInfoParser.class);
    private static final String VERSION;

    protected static final String DEFAULT_VALUE = "unknown";

    static {
        Properties props = new Properties();
        try (InputStream resourceStream = WaltzInfoParser.class.getResourceAsStream("/waltz/waltz-version.properties")) {
            props.load(resourceStream);
        } catch (Exception e) {
            logger.warn("Error while loading waltz-version.properties: {}", e.getMessage());
        }
        VERSION = props.getProperty("version", DEFAULT_VALUE).trim();
    }

    private WaltzInfoParser() {}

    public static String getVersion() {
        return VERSION;
    }
}
