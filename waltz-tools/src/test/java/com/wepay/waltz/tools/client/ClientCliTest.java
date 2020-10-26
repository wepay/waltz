package com.wepay.waltz.tools.client;

import com.wepay.waltz.client.WaltzClientConfig;
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
import static org.junit.Assert.assertTrue;

public class ClientCliTest {

    private static final int CLUSTER_NUM_PARTITIONS = 3;
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

    Properties createProperties(String connectString,  String znodePath, int sessionTimeout, SslSetup sslSetup) {
        return createProperties(connectString, znodePath, sessionTimeout, sslSetup,
            WaltzClientConfig.CLIENT_SSL_CONFIG_PREFIX);
    }

    Properties createProperties(String connectString, String znodePath, int sessionTimeout, SslSetup sslSetup,
                                String sslConfigPrefix) {
        Properties properties =  new Properties();
        properties.setProperty(CliConfig.ZOOKEEPER_CONNECT_STRING, connectString);
        properties.setProperty(CliConfig.ZOOKEEPER_SESSION_TIMEOUT, String.valueOf(sessionTimeout));
        properties.setProperty(CliConfig.CLUSTER_ROOT, znodePath);
        sslSetup.setConfigParams(properties, sslConfigPrefix);

        return properties;
    }

    @Test
    public void testSubmit() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/client/cli/test");

        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(CLUSTER_NUM_PARTITIONS));
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
                                                    helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();
        helper.startWaltzStorage(true);

        String storage = helper.getHost() + ":" + helper.getStorageAdminPort();

        addPartitionsToStorage(CLUSTER_NUM_PARTITIONS, storage, helper);

        helper.startWaltzServer(true);

        try {
            // validate partition that is clean
            String[] args1 = {
                    "validate",
                    "--txn-per-client", "50",
                    "--num-clients", "2",
                    "--interval", "20",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(args1);
            assertFalse(errContent.toString("UTF-8").contains("Error"));

            // validate partition that data exists
            String[] args2 = {
                    "validate",
                    "--txn-per-client", "50",
                    "--num-clients", "2",
                    "--interval", "20",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(args2);
            assertFalse(errContent.toString("UTF-8").contains("Error"));
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testSubmitWithMultiplePartitions() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/client/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(CLUSTER_NUM_PARTITIONS));
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
                helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();
        helper.startWaltzStorage(true);

        String storage = helper.getHost() + ":" + helper.getStorageAdminPort();
        addPartitionsToStorage(CLUSTER_NUM_PARTITIONS, storage, helper);

        helper.startWaltzServer(true);

        try {
            // validate with multi-partitions
            String[] args1 = {
                    "validate",
                    "--txn-per-client", "50",
                    "--num-clients", "2",
                    "--interval", "20",
                    "--num-active-partitions", "3",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(args1);
            assertFalse(errContent.toString("UTF-8").contains("Error"));
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testCreateProducerConsumer() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/client/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(CLUSTER_NUM_PARTITIONS));
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
                helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();
        helper.startWaltzStorage(true);

        String storage = helper.getHost() + ":" + helper.getStorageAdminPort();
        addPartitionsToStorage(CLUSTER_NUM_PARTITIONS, storage, helper);

        helper.startWaltzServer(true);

        try {
            // validate with multi-partitions
            String[] producer1 = {
                    "create-producer",
                    "--txn-per-client", "50",
                    "--interval", "20",
                    "--num-active-partitions", "3",
                    "--cli-config-path", configFilePath
            };
            String[] producer2 = {
                    "create-producer",
                    "--txn-per-client", "50",
                    "--interval", "20",
                    "--num-active-partitions", "3",
                    "--cli-config-path", configFilePath
            };
            String[] consumer1 = {
                    "create-consumer",
                    "--txn-per-client", "100",
                    "--num-active-partitions", "3",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(producer1);
            ClientCli.testMain(producer2);
            ClientCli.testMain(consumer1);

            assertFalse(errContent.toString("UTF-8").contains("Error"));
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testGetHighWaterMark() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/client/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(CLUSTER_NUM_PARTITIONS));
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
                                                    helper.getZkSessionTimeout(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();
        helper.startWaltzStorage(true);

        String storage = helper.getHost() + ":" + helper.getStorageAdminPort();
        addPartitionsToStorage(CLUSTER_NUM_PARTITIONS, storage, helper);

        helper.startWaltzServer(true);

        try {
            // update high watermark
            String[] args1 = {
                    "validate",
                    "--txn-per-client", "50",
                    "--num-clients", "2",
                    "--interval", "20",
                    "--num-active-partitions", "1",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(args1);

            // get high watermark
            String[] args2 = {
                    "high-water-mark",
                    "--partition", "0",
                    "--cli-config-path", configFilePath
            };
            ClientCli.testMain(args2);

            assertFalse(errContent.toString("UTF-8").contains("Error"));
            assertTrue(outContent.toString("UTF-8").contains("Partition 0 current high watermark: 99"));
        } finally {
            helper.closeAll();
        }
    }

    private void addPartitionsToStorage(int numPartitions, String storage, IntegrationTestHelper helper) throws Exception {
        Properties storageConfigProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(),
            helper.getZkSessionTimeout(), helper.getSslSetup(), CliConfig.SSL_CONFIG_PREFIX);
        String storageConfigFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME,
            storageConfigProperties);
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            String[] args = {
                    "add-partition",
                    "--storage", storage,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", storageConfigFilePath
            };
            StorageCli.testMain(args);
        }
    }

}
