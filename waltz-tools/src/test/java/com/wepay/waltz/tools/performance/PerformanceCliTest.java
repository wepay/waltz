package com.wepay.waltz.tools.performance;

import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.SslSetup;
import com.wepay.waltz.tools.CliConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class PerformanceCliTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    private static final String DIR_NAME = "performanceCliTest";
    private static final String CONFIG_FILE_NAME = "test-config.yml";

    @Before
    public void setUpStreams() throws UnsupportedEncodingException {
        System.setOut(new PrintStream(outContent, false, "UTF-8"));
        System.setErr(new PrintStream(errContent, false, "UTF-8"));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    Properties createProperties(String connectString, String znodePath, int sessionTimeout, SslSetup sslSetup) {
        Properties properties =  new Properties();
        properties.setProperty(CliConfig.ZOOKEEPER_CONNECT_STRING, connectString);
        properties.setProperty(CliConfig.ZOOKEEPER_SESSION_TIMEOUT, String.valueOf(sessionTimeout));
        properties.setProperty(CliConfig.CLUSTER_ROOT, znodePath);
        sslSetup.setConfigParams(properties, CliConfig.SSL_CONFIG_PREFIX);

        return properties;
    }

    @Test
    public void testRunProducer() throws Exception {
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/producer/perf/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "3");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");

        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
                helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);
        String[] args = {
                "test-producers",
                "--txn-size",
                "128",
                "--txn-per-thread",
                "10",
                "--num-thread",
                "2",
                "--interval",
                "10",
                "--cli-config-path",
                configFilePath,
                "--lock-pool-size",
                "2",
                "--num-active-partitions",
                "1"
        };
        try {
            helper.startZooKeeperServer();
            helper.startWaltzStorage(true);
            helper.setWaltzStorageAssignment(true);
            helper.startWaltzServer(true);

            PerformanceCli.testMain(args);

            String expectedCmdOutput = "Appended 20 transactions";
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testRunConsumer() throws Exception {
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/consumer/perf/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "3");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");

        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
                helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);
        String[] args = {
                "test-consumers",
                "--txn-size",
                "128",
                "--num-txn",
                "10",
                "--cli-config-path",
                configFilePath,
                "--num-active-partitions",
                "1"
        };

        try {
            helper.startZooKeeperServer();
            helper.startWaltzStorage(true);
            helper.setWaltzStorageAssignment(true);
            helper.startWaltzServer(true);

            PerformanceCli.testMain(args);

            String expectedCmdOutput = "Read 10 transactions";
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));
            outContent.reset();

            String[] argsMountLatest = {
                    "test-consumers",
                    "--txn-size",
                    "128",
                    "--num-txn",
                    "10",
                    "--cli-config-path",
                    configFilePath,
                    "--num-active-partitions",
                    "1",
                    "--mount_from_latest"
            };
            PerformanceCli.testMain(argsMountLatest);

            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));

        } finally {
            helper.closeAll();
        }
    }

}
