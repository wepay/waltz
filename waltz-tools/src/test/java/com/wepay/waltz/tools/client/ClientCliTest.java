package com.wepay.waltz.tools.client;

import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.SslSetup;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.waltz.tools.storage.StorageCli;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import static org.junit.Assert.assertFalse;

public class ClientCliTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    private static final String DIR_NAME = "clientCliTest";
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
    public void testSubmit() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/client/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "1");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
                                                    helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();
        helper.startWaltzStorage(true);

        int partitionToTest = 0;
        String[] args = {
                "add-partition",
                "--storage", helper.getHost() + ":" + helper.getStorageAdminPort(),
                "--partition", String.valueOf(partitionToTest),
                "--cli-config-path", configFilePath
        };
        StorageCli.testMain(args);

        helper.startWaltzServer(true);

        try {
            String[] args1 = {
                    "validate",
                    "--partition", String.valueOf(partitionToTest),
                    "--txn-per-client", "50",
                    "--num-client", "2",
                    "--interval", "20",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(args1);
            assertFalse(errContent.toString("UTF-8").contains("Error"));

            // validate cmd with --high-watermark option
            String[] args2 = {
                    "validate",
                    "--partition", String.valueOf(partitionToTest),
                    "--high-watermark", "99",
                    "--txn-per-client", "50",
                    "--num-client", "2",
                    "--interval", "20",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(args2);
            assertFalse(errContent.toString("UTF-8").contains("Error"));
        } finally {
            helper.closeAll();
        }
    }
}
