package com.wepay.waltz.tools.cluster;

import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.SslSetup;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.waltz.tools.storage.StorageCli;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterCliTest {
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

    private Properties createProperties(String connectString, String znodePath, int sessionTimeout, SslSetup sslSetup) {
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

    @Test
    public void testVerifyCommand() throws Exception {
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
        String host = InetAddress.getLocalHost().getCanonicalHostName();
        int partitionId = 1;
        try {
            helper.startZooKeeperServer();

            for (int i = 0; i < numStorageNodes; i++) {
                WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner(i);
                storageRunner.startAsync();
                storageRunner.awaitStart();

                // Add partition P1 to all storage nodes.
                String[] args0 = {
                    "add-partition",
                    "--storage", host + ":" + helper.getStorageAdminPort(i),
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
                };
                StorageCli.testMain(args0);
            }

            helper.startWaltzServer(true);

            String[] args1 = {
                    "verify",
                    "--cli-config-path", configFilePath
            };
            ClusterCli.testMain(args1);

            // Check that the server partition assignment on ZooKeeper matches with that on the server node.
            assertFalse(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY failed"));

            // Check that server to storage connectivity didn't fail.
            assertFalse(outContent.toString("UTF-8")
                .contains("Validation SERVER_STORAGE_CONNECTIVITY failed"));

            // Check that the storage partition assignment on ZooKeeper match with that on the storage nodes only for
            // Partition 1.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_STORAGE_CONSISTENCY failed for partition 0"));
            assertFalse(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_STORAGE_CONSISTENCY failed for partition 1"));
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_STORAGE_CONSISTENCY failed for partition 2"));

            // Check that quorum is only achieved on Partition 1.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_QUORUM_STATUS failed for partition 0"));
            assertFalse(outContent.toString("UTF-8")
                .contains("Validation PARTITION_QUORUM_STATUS failed for partition 1"));
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_QUORUM_STATUS failed for partition 2"));

            // Check that REPLICA_RECOVERY_STATUS verification didn't fail
            assertFalse(outContent.toString("UTF-8").contains("Recovery not completed"));
            assertFalse(outContent.toString("UTF-8").contains("Cannot obtain replica states"));

            // Close the server network connection
            WaltzServerRunner waltzServerRunner = helper.getWaltzServerRunner(helper.getServerPort(),
                    helper.getServerJettyPort());
            waltzServerRunner.closeNetworkServer();

            // Stop the storage node [0]
            WaltzStorageRunner waltzStorageRunner = helper.getWaltzStorageRunner();
            waltzStorageRunner.stop();

            // Stop the storage node [1]. (Note: Quorum should fail for Partition 1.)
            waltzStorageRunner = helper.getWaltzStorageRunner(1);
            waltzStorageRunner.stop();

            String[] args2 = {
                    "verify",
                    "--cli-config-path", configFilePath
            };
            ClusterCli.testMain(args2);

            // Check that the server partition assignment on ZooKeeper doesn't match with that on the server.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY failed"));

            // Check that server to storage connectivity failed.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation SERVER_STORAGE_CONNECTIVITY failed"));

            // Check that for partition 1, the storage partition assignment on ZooKeeper doesn't match with that on
            // the storage nodes.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_STORAGE_CONSISTENCY failed for partition 1"));

            // Check that quorum is not achieved on Partition 1.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_QUORUM_STATUS failed for partition 1"));
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testVerifyCommandWithPartitionOption() throws Exception {
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
        String host = InetAddress.getLocalHost().getCanonicalHostName();
        int partitionId = 1;
        try {
            helper.startZooKeeperServer();

            for (int i = 0; i < numStorageNodes; i++) {
                WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner(i);
                storageRunner.startAsync();
                storageRunner.awaitStart();

                // Add partition P1 to all storage nodes.
                String[] args0 = {
                    "add-partition",
                    "--storage", host + ":" + helper.getStorageAdminPort(i),
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
                };
                StorageCli.testMain(args0);
            }

            helper.startWaltzServer(true);

            String[] args1 = {
                "verify",
                "--cli-config-path", configFilePath,
                "--partition", String.valueOf(partitionId)
            };
            ClusterCli.testMain(args1);

            // Check that the server partition assignment on ZooKeeper matches with that on the server node.
            assertFalse(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY failed for partition " + partitionId));

            // Check that server to storage connectivity didn't fail.
            assertFalse(outContent.toString("UTF-8")
                .contains("Validation SERVER_STORAGE_CONNECTIVITY failed for partition " + partitionId));

            // Check that REPLICA_RECOVERY_STATUS verification didn't fail
            assertFalse(outContent.toString("UTF-8").contains("Recovery not completed"));
            assertFalse(outContent.toString("UTF-8").contains("Cannot obtain replica states"));

            // Close the server network connection
            WaltzServerRunner waltzServerRunner = helper.getWaltzServerRunner(helper.getServerPort(),
                helper.getServerJettyPort());
            waltzServerRunner.closeNetworkServer();

            // Stop the storage node [0]
            WaltzStorageRunner waltzStorageRunner = helper.getWaltzStorageRunner();
            waltzStorageRunner.stop();

            // Stop the storage node [1]. (Note: Quorum should fail for Partition 1.)
            waltzStorageRunner = helper.getWaltzStorageRunner(1);
            waltzStorageRunner.stop();

            String[] args2 = {
                "verify",
                "--cli-config-path", configFilePath,
                "--partition", String.valueOf(partitionId)
            };
            ClusterCli.testMain(args2);

            // Check that the server partition assignment on ZooKeeper doesn't match with that on the server.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation PARTITION_ASSIGNMENT_ZK_SERVER_CONSISTENCY failed for partition " + partitionId));

            // Check that server to storage connectivity failed.
            assertTrue(outContent.toString("UTF-8")
                .contains("Validation SERVER_STORAGE_CONNECTIVITY failed for partition " + partitionId));

        } finally {
            helper.closeAll();
        }
    }
}
