package com.wepay.riff.metrics.graphite;

import com.wepay.riff.config.ConfigException;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GraphiteReporterConfigTest {
    @Test
    public void testDefaultValues() {
        Properties props = new Properties();
        GraphiteReporterConfig config = new GraphiteReporterConfig("", props);
        Object value;

        try {
            config.get(GraphiteReporterConfig.HOSTNAME);
            fail();
        } catch (ConfigException ex) {
            // Ignore
        } catch (Exception ex) {
            fail();
        }

        assertEquals(GraphiteReporterConfig.DEFAULT_PORT, config.get(GraphiteReporterConfig.PORT));
        assertEquals(GraphiteReporterConfig.DEFAULT_PREFIX, config.get(GraphiteReporterConfig.PREFIX));
        assertEquals(GraphiteReporterConfig.DEFAULT_REPORT_INTERVAL_SECONDS, config.get(GraphiteReporterConfig.REPORT_INTERVAL_SECONDS));
        assertEquals(GraphiteReporterConfig.DEFAULT_USE_UDP, config.get(GraphiteReporterConfig.USE_UDP));
    }
}
