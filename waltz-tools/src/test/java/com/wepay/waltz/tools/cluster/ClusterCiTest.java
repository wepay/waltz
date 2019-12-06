package com.wepay.waltz.tools.cluster;

import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.SslSetup;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import com.wepay.waltz.tools.CliConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class ClusterCiTest {
    private static final String DIR_NAME = "clusterCliTest";
    private static final String CONFIG_FILE_NAME = "test-config.yml";

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @Before
    public void setUpStreams() throws UnsupportedEncodingException {
        System.setOut(new PrintStream(outContent, false, "UTF-8"));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
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
    public void testCheckConnectivity() throws Exception {
        int numPartitions = 3;
        int numStorageNodes = 3;
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/cluster/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(numPartitions));
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        properties.setProperty(IntegrationTestHelper.Config.NUM_STORAGES, String.valueOf(numStorageNodes));
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);

        Properties configProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
            helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME,
            configProperties);
        Map<String, Boolean> expectedStorageConnectivity = new HashMap<>();
        try {
            helper.startZooKeeperServer();
            for (int i = 0; i < numStorageNodes; i++) {
                WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner(i);
                storageRunner.startAsync();
                storageRunner.awaitStart();
                expectedStorageConnectivity.put(helper.getStorageConnectString(i), true);
            }
            helper.startWaltzServer(true);

            String[] args1 = {
                "check-connectivity",
                "--cli-config-path", configFilePath
            };
            ClusterCli.testMain(args1);
            String expectedCmdOutput = "Connectivity status of " + helper.getHost() + ":" + helper.getServerPort()
                + " is: " + expectedStorageConnectivity.toString();
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));

            // Close one storage node.
            Random rand = new Random();
            int storageNodeId = rand.nextInt(numStorageNodes);
            WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner(storageNodeId);
            storageRunner.stop();
            expectedStorageConnectivity.put(helper.getStorageConnectString(storageNodeId), false);
            String[] args2 = {
                "check-connectivity",
                "--cli-config-path", configFilePath
            };
            ClusterCli.testMain(args2);
            expectedCmdOutput = "Connectivity status of " + helper.getHost() + ":" + helper.getServerPort()
                + " is: " + expectedStorageConnectivity.toString();
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));

            // Close the server network connection
            WaltzServerRunner waltzServerRunner = helper.getWaltzServerRunner(helper.getServerPort(),
                helper.getServerJettyPort());
            waltzServerRunner.closeNetworkServer();
            String[] args3 = {
                "check-connectivity",
                "--cli-config-path", configFilePath
            };
            ClusterCli.testMain(args3);
            expectedCmdOutput = "Connectivity status of " + helper.getHost() + ":" + helper.getServerPort()
                + " is: UNREACHABLE";
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));
        } finally {
            helper.closeAll();
        }
    }
}
