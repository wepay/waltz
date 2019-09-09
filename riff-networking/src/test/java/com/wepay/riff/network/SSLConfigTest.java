package com.wepay.riff.network;

import com.wepay.riff.config.ConfigException;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SSLConfigTest {

    @Test
    public void testDefaultValues() {
        Properties props = new Properties();
        SSLConfig config = new SSLConfig("", props);
        Object value;

        try {
            config.get(SSLConfig.KEY_STORE_LOCATION);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            config.get(SSLConfig.KEY_STORE_PASSWORD);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        value = config.get(SSLConfig.KEY_STORE_TYPE);
        assertTrue(value instanceof String);
        assertEquals(SSLConfig.DEFAULT_KEY_STORE_TYPE, value);

        try {
            config.get(SSLConfig.TRUST_STORE_LOCATION);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        try {
            config.get(SSLConfig.TRUST_STORE_PASSWORD);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        value = config.get(SSLConfig.TRUST_STORE_TYPE);
        assertTrue(value instanceof String);
        assertEquals(SSLConfig.DEFAULT_TRUST_STORE_TYPE, value);

        value = config.get(SSLConfig.KEY_MANAGER_ALGORITHM);
        assertTrue(value instanceof String);
        assertEquals(SSLConfig.DEFAULT_KEY_MANAGER_ALGORITHM, value);

        value = config.get(SSLConfig.TRUST_MANAGER_ALGORITHM);
        assertTrue(value instanceof String);
        assertEquals(SSLConfig.DEFAULT_TRUST_MANAGER_ALGORITHM, value);
    }

}
