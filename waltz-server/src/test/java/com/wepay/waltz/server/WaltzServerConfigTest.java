package com.wepay.waltz.server;

import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.config.ConfigException;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WaltzServerConfigTest {

    @Test
    public void testParsers() {
        Map<Object, Object> map = new HashMap<>();

        map.put(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING, "fakehost:9999");
        map.put(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT, "1000");

        map.put(WaltzServerConfig.CLUSTER_ROOT, "/cluster");
        map.put(WaltzServerConfig.SERVER_PORT, "8888");

        map.put(WaltzServerConfig.OPTIMISTIC_LOCK_TABLE_SIZE, "2000");
        map.put(WaltzServerConfig.STORE_SESSION_BATCH_SIZE, "100");
        map.put(WaltzServerConfig.FEED_CACHE_SIZE, "1000");
        map.put(WaltzServerConfig.MIN_FETCH_SIZE, "50");
        map.put(WaltzServerConfig.REALTIME_THRESHOLD, "500");

        map.put(WaltzServerConfig.INITIAL_RETRY_INTERVAL, "30");
        map.put(WaltzServerConfig.MAX_RETRY_INTERVAL, "30000");

        WaltzServerConfig config = new WaltzServerConfig(map);
        Object value;

        value = config.get(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING);
        assertTrue(value instanceof String);
        assertEquals("fakehost:9999", value);

        value = config.get(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT);
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        value = config.get(WaltzServerConfig.CLUSTER_ROOT);
        assertTrue(value instanceof String);
        assertEquals("/cluster", value);

        value = config.get(WaltzServerConfig.SERVER_PORT);
        assertTrue(value instanceof Integer);
        assertEquals(8888, value);

        value = config.get(WaltzServerConfig.OPTIMISTIC_LOCK_TABLE_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(2000, value);

        value = config.get(WaltzServerConfig.STORE_SESSION_BATCH_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(100, value);

        value = config.get(WaltzServerConfig.FEED_CACHE_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(1000, value);

        value = config.get(WaltzServerConfig.MIN_FETCH_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(50, value);

        value = config.get(WaltzServerConfig.REALTIME_THRESHOLD);
        assertTrue(value instanceof Integer);
        assertEquals(500, value);

        value = config.get(WaltzServerConfig.INITIAL_RETRY_INTERVAL);
        assertTrue(value instanceof Long);
        assertEquals(30L, value);

        value = config.get(WaltzServerConfig.MAX_RETRY_INTERVAL);
        assertTrue(value instanceof Long);
        assertEquals(30000L, value);
    }

    @Test
    public void testDefaultValues() {
        WaltzServerConfig config = new WaltzServerConfig(Collections.emptyMap());
        Object value;

        try {
            value = config.get(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            value = config.get(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            value = config.get(WaltzServerConfig.SERVER_PORT);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        value = config.get(WaltzServerConfig.OPTIMISTIC_LOCK_TABLE_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzServerConfig.DEFAULT_OPTIMISTIC_LOCK_TABLE_SIZE, value);

        value = config.get(WaltzServerConfig.STORE_SESSION_BATCH_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzServerConfig.DEFAULT_STORE_SESSION_BATCH_SIZE, value);

        value = config.get(WaltzServerConfig.FEED_CACHE_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzServerConfig.DEFAULT_FEED_CACHE_SIZE, value);

        value = config.get(WaltzServerConfig.MIN_FETCH_SIZE);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzServerConfig.DEFAULT_MIN_FETCH_SIZE, value);

        value = config.get(WaltzServerConfig.REALTIME_THRESHOLD);
        assertTrue(value instanceof Integer);
        assertEquals(WaltzServerConfig.DEFAULT_REALTIME_THRESHOLD, value);

        value = config.get(WaltzServerConfig.INITIAL_RETRY_INTERVAL);
        assertTrue(value instanceof Long);
        assertEquals(WaltzServerConfig.DEFAULT_INITIAL_RETRY_INTERVAL, value);

        value = config.get(WaltzServerConfig.MAX_RETRY_INTERVAL);
        assertTrue(value instanceof Long);
        assertEquals(WaltzServerConfig.DEFAULT_MAX_RETRY_INTERVAL, value);
    }

    @Test
    public void testSSLConfigs() {
        testSSLConfigs("");
        testSSLConfigs("test.");
    }

    private void testSSLConfigs(String prefix) {
        Map<Object, Object> map = new HashMap<>();
        map.put(prefix + WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX + SSLConfig.KEY_STORE_LOCATION, "/ks");

        WaltzServerConfig serverConfig = new WaltzServerConfig(prefix, map);
        SSLConfig config;
        Object value;

        config = serverConfig.getSSLConfig();
        value = config.get(SSLConfig.KEY_STORE_LOCATION);
        assertTrue(value instanceof String);
        assertEquals("/ks", value);
    }

    @Test
    public void testYamlNestedMap() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (OutputStreamWriter writer = new OutputStreamWriter(baos, StandardCharsets.UTF_8)) {
            writer.write("zookeeper:\n");
            writer.write(" connectString: fakehost:9999\n");
            writer.write(" sessionTimeout: 30000\n");
            writer.write("cluster.root: /cluster\n");
            writer.write("server:\n");
            writer.write("  port: 8888\n");
            writer.write("storage:\n");
            writer.write("  initialRetryInterval: 1000\n");
            writer.write("  maxRetryInterval: \"10000\"\n");
        }

        try (InputStream input = new ByteArrayInputStream(baos.toByteArray())) {
            WaltzServerConfig config = new WaltzServerConfig(new Yaml().load(input));
            Object value;

            value = config.get(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING);
            assertTrue(value instanceof String);
            assertEquals("fakehost:9999", value);

            value = config.get(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT);
            assertTrue(value instanceof Integer);
            assertEquals(30000, value);

            value = config.get(WaltzServerConfig.CLUSTER_ROOT);
            assertTrue(value instanceof String);
            assertEquals("/cluster", value);

            value = config.get(WaltzServerConfig.SERVER_PORT);
            assertTrue(value instanceof Integer);
            assertEquals(8888, value);

            value = config.get(WaltzServerConfig.INITIAL_RETRY_INTERVAL);
            assertTrue(value instanceof Long);
            assertEquals(1000L, value);

            value = config.get(WaltzServerConfig.MAX_RETRY_INTERVAL);
            assertTrue(value instanceof Long);
            assertEquals(10000L, value);
        }
    }

    @Test
    public void testYamlReference() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (OutputStreamWriter writer = new OutputStreamWriter(baos, StandardCharsets.UTF_8)) {
            writer.write("commonSsl: &ssl\n");
            writer.write("   keyStore:\n");
            writer.write("     location: /ks\n");
            writer.write("     password: secret\n");
            writer.write("server:\n");
            writer.write(" ssl: *ssl\n");
            writer.write("storage:\n");
            writer.write("  ssl: *ssl\n");
        }

        try (InputStream input = new ByteArrayInputStream(baos.toByteArray())) {
            WaltzServerConfig config = new WaltzServerConfig(new Yaml().load(input));
            SSLConfig sslConfig;

            sslConfig = config.getSSLConfig();
            assertEquals("/ks", sslConfig.get(SSLConfig.KEY_STORE_LOCATION));
            assertEquals("secret", sslConfig.get(SSLConfig.KEY_STORE_PASSWORD));

            sslConfig = config.getSSLConfig();
            assertEquals("/ks", sslConfig.get(SSLConfig.KEY_STORE_LOCATION));
            assertEquals("secret", sslConfig.get(SSLConfig.KEY_STORE_PASSWORD));
        }
    }

}
