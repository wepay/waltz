package com.wepay.waltz.client;

import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.config.ConfigException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WaltzClientConfigTest {

    @Test
    public void testParsers() {
        Map<Object, Object> map = new HashMap<>();
        map.put(WaltzClientConfig.ZOOKEEPER_CONNECT_STRING, "fakehost:9999");
        map.put(WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT, "1000");
        map.put(WaltzClientConfig.CLUSTER_ROOT, "/cluster");

        WaltzClientConfig config = new WaltzClientConfig(map);
        Object value;

        value = config.get(WaltzClientConfig.ZOOKEEPER_CONNECT_STRING);
        assertTrue(value instanceof String);
        assertEquals("fakehost:9999", value);

        value = config.get(WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT);
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        value = config.get(WaltzClientConfig.CLUSTER_ROOT);
        assertTrue(value instanceof String);
        assertEquals("/cluster", value);
    }

    @Test
    public void testDefaultValues() {
        WaltzClientConfig config = new WaltzClientConfig(Collections.emptyMap());
        Object value;

        try {
            config.get(WaltzClientConfig.ZOOKEEPER_CONNECT_STRING);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            config.get(WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            config.get(WaltzClientConfig.CLUSTER_ROOT);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        value = config.get(WaltzClientConfig.NUM_TRANSACTION_RETRY_THREADS);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzClientConfig.DEFAULT_NUM_TRANSACTION_RETRY_THREADS, value);

        value = config.get(WaltzClientConfig.NUM_CONSUMER_THREADS);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzClientConfig.DEFAULT_NUM_CONSUMER_THREADS, value);

        value = config.get(WaltzClientConfig.MAX_CONCURRENT_TRANSACTIONS);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, value);
    }

    @Test
    public void testSSLConfig() {
        testSSLConfig("");
        testSSLConfig("test.");
    }

    private void testSSLConfig(String prefix) {
        Map<Object, Object> map = new HashMap<>();
        map.put(prefix + WaltzClientConfig.CLIENT_SSL_CONFIG_PREFIX + SSLConfig.KEY_STORE_LOCATION, "/ks");

        SSLConfig config = new WaltzClientConfig(prefix, map).getSSLConfig();
        Object value;

        value = config.get(SSLConfig.KEY_STORE_LOCATION);
        assertTrue(value instanceof String);
        assertEquals("/ks", value);
    }

}
