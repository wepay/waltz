package com.wepay.waltz.storage.server;

import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.config.ConfigException;
import com.wepay.waltz.storage.WaltzStorageConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WaltzStorageConfigTest {

    @Test
    public void testParsers() {
        Map<Object, Object> map = new HashMap<>();
        map.put(WaltzStorageConfig.STORAGE_PORT, "8000");
        map.put(WaltzStorageConfig.STORAGE_ADMIN_PORT, "8100");
        map.put(WaltzStorageConfig.STORAGE_DIRECTORY, "/storage/dir");
        map.put(WaltzStorageConfig.STORAGE_JETTY_PORT, "9000");

        WaltzStorageConfig config = new WaltzStorageConfig(map);
        Object value;

        value = config.get(WaltzStorageConfig.STORAGE_PORT);
        assertTrue(value instanceof Integer);
        assertEquals(8000, value);

        value = config.get(WaltzStorageConfig.STORAGE_ADMIN_PORT);
        assertTrue(value instanceof Integer);
        assertEquals(8100, value);

        value = config.get(WaltzStorageConfig.STORAGE_DIRECTORY);
        assertTrue(value instanceof String);
        assertEquals("/storage/dir", value);

        value = config.get(WaltzStorageConfig.STORAGE_JETTY_PORT);
        assertTrue(value instanceof Integer);
        assertEquals(9000, value);

    }

    @Test
    public void testDefaultValues() {
        WaltzStorageConfig config = new WaltzStorageConfig(Collections.emptyMap());
        Object value;

        try {
            value = config.get(WaltzStorageConfig.STORAGE_PORT);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            value = config.get(WaltzStorageConfig.STORAGE_ADMIN_PORT);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            value = config.get(WaltzStorageConfig.STORAGE_DIRECTORY);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            value = config.get(WaltzStorageConfig.STORAGE_JETTY_PORT);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        value = config.get(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD);
        assertTrue(value instanceof Long);
        assertEquals(WaltzStorageConfig.DEFAULT_SEGMENT_SIZE_THRESHOLD, value);
    }

    @Test
    public void testSSLConfigs() {
        testSSLConfigs("");
        testSSLConfigs("test.");
    }

    private void testSSLConfigs(String prefix) {
        Map<Object, Object> map = new HashMap<>();
        map.put(prefix + WaltzStorageConfig.STORAGE_SSL_CONFIG_PREFIX + SSLConfig.KEY_STORE_LOCATION, "/ks");

        WaltzStorageConfig storageConfig = new WaltzStorageConfig(prefix, map);
        SSLConfig config;
        Object value;

        config = storageConfig.getSSLConfig();
        value = config.get(SSLConfig.KEY_STORE_LOCATION);
        assertTrue(value instanceof String);
        assertEquals("/ks", value);
    }

}
