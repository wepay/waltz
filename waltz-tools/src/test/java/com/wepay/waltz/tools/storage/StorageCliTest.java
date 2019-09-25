package com.wepay.waltz.tools.storage;

import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.storage.WaltzStorage;
import com.wepay.waltz.storage.WaltzStorageConfig;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.storage.server.internal.PartitionInfoSnapshot;
import com.wepay.waltz.store.internal.metadata.StoreMetadata;
import com.wepay.waltz.test.util.ClientUtil;
import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.SslSetup;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import com.wepay.waltz.test.util.ZooKeeperServerRunner;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.waltz.tools.zk.ZooKeeperCli;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import io.netty.handler.ssl.SslContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class StorageCliTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    private static final String DIR_NAME = "storageCliTest";
    private static final String CONFIG_FILE_NAME = "test-config.yml";
    private static final String SOURCE_SSL_CONFIG_FILE_NAME = "test-source-ssl-config.yml";
    private static final String DESTINATION_SSL_CONFIG_FILE_NAME = "test-destination-ssl-config.yml";

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

    Properties createProperties(String connectString, String znodePath, SslSetup sslSetup) {
        return createProperties(connectString, znodePath, sslSetup, CliConfig.SSL_CONFIG_PREFIX);
    }

    Properties createProperties(String connectString, String znodePath, SslSetup sslSetup, String sslConfigPrefix) {
        Properties properties =  new Properties();
        properties.setProperty(CliConfig.ZOOKEEPER_CONNECT_STRING, connectString);
        properties.setProperty(CliConfig.CLUSTER_ROOT, znodePath);
        sslSetup.setConfigParams(properties, sslConfigPrefix);

        return properties;
    }

    @Test
    public void testListPartition() throws Exception {
        int numPartitions = 1;
        IntegrationTestHelper helper = getIntegrationTestHelper(numPartitions);
        int partitionId = new Random().nextInt(helper.getNumPartitions());

        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();

        WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner();
        storageRunner.startAsync();
        WaltzStorage storage = storageRunner.awaitStart();
        int adminPort = storage.adminPort;
        String adminConnectString = helper.getHost() + ":" + adminPort;

        helper.startWaltzServer(true);

        // wait for storage node partitions creation
        Thread.sleep(1000);

        // default partition assignment is set to True
        PartitionInfoSnapshot info = storage
                .getPartitionInfos()
                .stream()
                .filter(pis -> pis.partitionId == partitionId)
                .findFirst()
                .get();
        assertFalse(info.isAssigned);

        try {
            // add partition to storage
            String[] args1 = {
                    "add-partition",
                    "--storage", adminConnectString,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args1);

            // successful path
            String[] args2 = {
                    "list",
                    "--storage", adminConnectString,
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args2);

            String expectedCmdOutput = "Partition Info for id: " + partitionId;
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));

            // failure path
            String[] args3 = {
                    "list",
                    "--storage", "badhost.local:" + adminPort,
                    "--cli-config-path", configFilePath
            };
            StorageCli.testMain(args3);
            expectedCmdOutput = "Error: Cannot fetch partition ownership for badhost.local:" + adminPort;
            assertTrue(errContent.toString("UTF-8").contains(expectedCmdOutput));
        } finally {
            helper.closeAll();
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testAddAndRemovePartition() throws Exception {
        int numPartitions = 3;
        IntegrationTestHelper helper = getIntegrationTestHelper(numPartitions);
        int partitionId = new Random().nextInt(helper.getNumPartitions());

        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();

        WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner();
        storageRunner.startAsync();
        WaltzStorage storage = storageRunner.awaitStart();
        int adminPort = storage.adminPort;
        String adminConnectString = helper.getHost() + ":" + adminPort;

        helper.startWaltzServer(true);

        // wait for storage node partitions creation
        Thread.sleep(1000);

        // default partition assignment is set to True
        PartitionInfoSnapshot info = storage
                .getPartitionInfos()
                .stream()
                .filter(pis -> pis.partitionId == partitionId)
                .findFirst()
                .get();
        assertFalse(info.isAssigned);

        try {
            // add partition failure
            String[] args1 = {
                    "add-partition",
                    "--storage", "badhost.local:" + adminPort,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args1);
            info = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();
            assertFalse(info.isAssigned);

            // add partition success
            String[] args2 = {
                    "add-partition",
                    "--storage", adminConnectString,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args2);
            info = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();
            assertTrue(info.isAssigned);

            // remove partition failure
            String[] args3 = {
                    "remove-partition",
                    "--storage", "badhost.local:" + adminPort,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args3);
            info = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();
            assertTrue(info.isAssigned);

            // remove partition success
            String[] args4 = {
                    "remove-partition",
                    "--storage", adminConnectString,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args4);
            info = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();
            assertFalse(info.isAssigned);
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testRemovePartitionWithDeleteFilesOption() throws Exception {
        int numPartitions = 5;
        IntegrationTestHelper helper = getIntegrationTestHelper(numPartitions);

        int partitionId = new Random().nextInt(helper.getNumPartitions());

        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();

        WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner();
        storageRunner.startAsync();
        WaltzStorage storage = storageRunner.awaitStart();
        int adminPort = storage.adminPort;
        String adminConnectString = helper.getHost() + ":" + adminPort;

        helper.startWaltzServer(true);

        // wait for storage node partitions creation
        Thread.sleep(1000);

        // default partition assignment is set to True
        PartitionInfoSnapshot info = storage
                .getPartitionInfos()
                .stream()
                .filter(pis -> pis.partitionId == partitionId)
                .findFirst()
                .get();
        assertFalse(info.isAssigned);

        try {
            // add partition success
            String[] args1 = {
                    "add-partition",
                    "--storage", adminConnectString,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args1);
            info = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();
            assertTrue(info.isAssigned);

            // remove partition with -d option
            String[] args2 = {
                    "remove-partition",
                    "--storage", adminConnectString,
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath,
                    "--delete-storage-files"
            };

            StorageCli.testMain(args2);
            info = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();
            assertFalse(info.isAssigned);

            String fileNameFormat = "%019d.%s";
            Path partitionDir = storageRunner.directory().resolve(Integer.toString(partitionId));
            Path segPath = partitionDir.resolve(String.format(fileNameFormat, 0L, "seg"));
            Path idxPath = partitionDir.resolve(String.format(fileNameFormat, 0L, "idx"));

            // Verifies that the data and index files are deleted for the give partition when `--delete_storage_files` option is provided in the cli.
            assertFalse(segPath.toFile().exists());
            assertFalse(idxPath.toFile().exists());

        }  finally {
            helper.closeAll();
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testAvailability() throws Exception {
        int numPartitions = 3;
        IntegrationTestHelper helper = getIntegrationTestHelper(numPartitions);
        int partitionId = new Random().nextInt(helper.getNumPartitions());

        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();

        WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner();
        storageRunner.startAsync();
        WaltzStorage storage = storageRunner.awaitStart();
        int adminPort = storage.adminPort;
        String adminConnectString = helper.getHost() + ":" + adminPort;

        try {
            String[] args0 = {
                    "availability",
                    "--storage", adminConnectString,
                    "--partition", String.valueOf(partitionId),
                    "--online", String.valueOf(true),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args0);

            PartitionInfoSnapshot partitionInfoSnapshot0 = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();

            assertTrue(partitionInfoSnapshot0.isAvailable);

            String[] args1 = {
                    "availability",
                    "--storage", adminConnectString,
                    "--partition", String.valueOf(partitionId),
                    "--online", String.valueOf(false),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args1);

            PartitionInfoSnapshot partitionInfoSnapshot1 = storage
                    .getPartitionInfos()
                    .stream()
                    .filter(pis -> pis.partitionId == partitionId)
                    .findFirst()
                    .get();

            assertTrue(!partitionInfoSnapshot1.isAvailable);
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testRecoverPartition() throws Exception {
        long sessionId = 123;
        int numPartitions = 3;
        IntegrationTestHelper helper = getIntegrationTestHelper(numPartitions);
        String host = helper.getHost();

        SslContext sslCtx = helper.getSslContext();
        int partitionId = new Random().nextInt(helper.getNumPartitions());

        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        Properties sslCfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup(), WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
        String cliConfigPath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);
        String sourceSslConfigPath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, SOURCE_SSL_CONFIG_FILE_NAME, sslCfgProperties);
        String destinationSslConfigPath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, DESTINATION_SSL_CONFIG_FILE_NAME, sslCfgProperties);

        helper.startZooKeeperServer();

        PortFinder portFinder = new PortFinder();
        int destinationPort = portFinder.getPort();
        int destinationAdminPort = portFinder.getPort();
        int destinationJettyPort = portFinder.getPort();
        WaltzStorageRunner destinationStorageRunner = helper.getWaltzStorageRunner(destinationPort, destinationAdminPort, destinationJettyPort);
        WaltzStorageRunner sourceStorageRunner = helper.getWaltzStorageRunner();

        UUID key = helper.getClusterKey();

        try {
            sourceStorageRunner.startAsync();
            destinationStorageRunner.startAsync();

            WaltzStorage sourceWaltzStorage = sourceStorageRunner.awaitStart();
            WaltzStorage destinationWaltzStorage = destinationStorageRunner.awaitStart();

            int sourceAdminPort = sourceWaltzStorage.adminPort;

            // Create partitions on each storage node
            for (WaltzStorage waltzStorage : Utils.list(sourceWaltzStorage, destinationWaltzStorage)) {
                int adminPort = waltzStorage.adminPort;
                StorageAdminClient adminClient = helper.getStorageAdminClient(adminPort);
                adminClient.open();
                for (int i = 0; i < numPartitions; i++) {
                    adminClient.setPartitionAssignment(i, true, false).get();
                }
                adminClient.close();
            }

            // Load records into the source node
            ArrayList<Record> records = ClientUtil.makeRecords(0, 10);
            int sourcePort = sourceWaltzStorage.port;
            StorageClient sourceClient = new StorageClient(host, sourcePort, sslCtx, key, numPartitions);
            sourceClient.open();
            sourceClient.setLowWaterMark(sessionId, partitionId, -1L).get();
            sourceClient.appendRecords(sessionId, partitionId, records).get();
            sourceClient.setLowWaterMark(sessionId + 1, partitionId, records.size() - 1).get();

            // Validate destination node has no records
            StorageClient destinationClient = new StorageClient(host, destinationPort, sslCtx, key, numPartitions);
            destinationClient.open();
            Long maxTransactionId = (Long) destinationClient.getMaxTransactionId(-1, partitionId).get();
            assertNotNull(maxTransactionId);
            assertEquals(-1, maxTransactionId.longValue());

            // Set destination storage offline
            String[] args0 = {
                    "availability",
                    "--storage", host + ":" + destinationAdminPort,
                    "--partition", String.valueOf(partitionId),
                    "--online", String.valueOf(false),
                    "--cli-config-path", cliConfigPath
            };

            StorageCli.testMain(args0);

            // Recover destination node
            String[] args1 = {
                    "recover-partition",
                    "--source-storage", host + ":" + sourceAdminPort,
                    "--destination-storage", host + ":" + destinationAdminPort,
                    "--destination-storage-port", String.valueOf(destinationPort),
                    "--partition", String.valueOf(partitionId),
                    "--batch-size", String.valueOf(20),
                    "--cli-config-path", cliConfigPath,
                    "--source-ssl-config-path", sourceSslConfigPath,
                    "--destination-ssl-config-path", destinationSslConfigPath,
            };

            StorageCli.testMain(args1);

            // Set destination storage online
            String[] args2 = {
                    "availability",
                    "--storage", host + ":" + destinationAdminPort,
                    "--partition", String.valueOf(partitionId),
                    "--online", String.valueOf(true),
                    "--cli-config-path", cliConfigPath
            };

            StorageCli.testMain(args2);

            // Validate destination node has expected records
            maxTransactionId = (Long) destinationClient.getMaxTransactionId(-1, partitionId).get();
            assertNotNull(maxTransactionId);
            assertEquals(9, maxTransactionId.longValue());
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testSyncPartitionAssignments() throws Exception {
        int numStorage = 3;
        int numPartition = 12;
        int zkTimeout = 30000;
        int storageGroupId = 0;
        long segmentSizeThreshold = 400L;
        String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        PortFinder portFinder = new PortFinder();
        String znodePath = "/storage/cli/test";
        ZNode clusterRoot = new ZNode(znodePath);
        ZNode storeRoot = new ZNode(clusterRoot, StoreMetadata.STORE_ZNODE_NAME);

        SslSetup sslSetup = new SslSetup();
        Path sslDir = Files.createTempDirectory("waltz-integration-test");
        String sslConfigPath = IntegrationTestHelper.createYamlConfigFile(sslDir, "ssl.yaml", WaltzStorageConfig.STORAGE_SSL_CONFIG_PREFIX, sslSetup);
        SslContext sslContext = Utils.getSslContext(sslConfigPath, WaltzStorageConfig.STORAGE_SSL_CONFIG_PREFIX);

        ZooKeeperServerRunner zkServerRunner = null;
        ZooKeeperClient zkClient = null;
        List<WaltzStorageRunner> storageRunners = new ArrayList<>();
        Map<String, StorageClient> storageClientMap = new HashMap<>();

        try {
            // set up zookeeper server
            zkServerRunner = new ZooKeeperServerRunner(portFinder.getPort());
            String zkConnectionString = zkServerRunner.start();
            zkClient = new ZooKeeperClientImpl(zkConnectionString, zkTimeout);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRoot, "test cluster", numPartition);
            ZooKeeperCli.Create.createStores(zkClient, clusterRoot, numPartition);
            StoreMetadata storeMetadata = new StoreMetadata(zkClient, storeRoot);
            UUID key = storeMetadata.getStoreParams().key;

            for (int i = 0; i < numStorage; i++) {
                // set up storage node
                Properties storageProperties = new Properties();
                storageProperties.setProperty(WaltzStorageConfig.STORAGE_JETTY_PORT, String.valueOf(portFinder.getPort()));
                storageProperties.setProperty(WaltzStorageConfig.STORAGE_DIRECTORY, Files.createTempDirectory(DIR_NAME).toString());
                storageProperties.setProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD, String.valueOf(segmentSizeThreshold));
                storageProperties.setProperty(WaltzStorageConfig.CLUSTER_NUM_PARTITIONS, String.valueOf(numPartition));
                storageProperties.setProperty(WaltzStorageConfig.CLUSTER_KEY, String.valueOf(key));
                sslSetup.setConfigParams(storageProperties, WaltzStorageConfig.STORAGE_SSL_CONFIG_PREFIX);
                WaltzStorageConfig storageConfig = new WaltzStorageConfig(storageProperties);
                WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, storageConfig, segmentSizeThreshold);
                storageRunner.startAsync();
                WaltzStorage storage = storageRunner.awaitStart();
                String connectString = localhost + ":" + storage.port;

                // open storage node
                StorageClient storageClient = new StorageClient(localhost, storage.port, sslContext, key, numPartition);
                storageClient.open();

                // add storage node to group in ZK
                storeMetadata.addStorageNode(connectString, storageGroupId, storage.adminPort);

                storageClientMap.put(localhost + ":" + storage.port, storageClient);
                storageRunners.add(storageRunner);
            }

            // auto-assign partitions (partitions per storage = numPartition / numStorage = 12 / 3 = 4)
            storeMetadata.autoAssignPartition(storageGroupId);

            // verify storage nodes are not assigned
            for (Map.Entry<String, int[]> entry : storeMetadata.getReplicaAssignments().replicas.entrySet()) {
                String connectString = entry.getKey();
                int[] partitionIds = entry.getValue();
                for (int partitionId : partitionIds) {
                    try {
                        storageClientMap.get(connectString).lastSessionInfo(0L, partitionId).get();
                        fail("Unassigned storage client should fail to get last session info.");
                    } catch (ExecutionException e) {
                        Throwable t = e.getCause();
                        assertTrue(t.getMessage().contains("Partition:" + partitionId + " is not assigned."));
                    }
                }
            }

            // sync partition assignment
            Properties cfgProperties = createProperties(zkConnectionString, znodePath, sslSetup);
            String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);
            String[] args = {
                    "sync-partitions",
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args);

            // verify storage nodes are assigned
            for (Map.Entry<String, int[]> entry : storeMetadata.getReplicaAssignments().replicas.entrySet()) {
                for (int partitionId : entry.getValue()) {
                    storageClientMap.get(entry.getKey()).lastSessionInfo(0L, partitionId).get();
                }
            }
        } finally {
            // close storage
            for (WaltzStorageRunner runner : storageRunners) {
                runner.stop();
            }
            // cleanup znodes
            if (zkClient != null) {
                try {
                    zkClient.deleteRecursively(clusterRoot);
                } finally {
                    zkClient.close();
                }
            }
            // shut down zk server
            if (zkServerRunner != null) {
                zkServerRunner.stop();
            }
            // remove ssl
            sslSetup.close();
            // cleanup ssl directory
            Utils.removeDirectory(sslDir.toFile());
        }
    }

    @Test
    public void testValidateConnectivity() throws Exception {
        int numPartitions = 1;
        IntegrationTestHelper helper = getIntegrationTestHelper(numPartitions);

        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        String cliConfigPath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();

        WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner();
        storageRunner.startAsync();
        storageRunner.awaitStart();

        helper.startWaltzServer(true);

        // wait for storage node partitions creation
        Thread.sleep(1000);

        try {
            // test with valid connection
            String[] validateArgs = {
                    "validate",
                    "--cli-config-path", cliConfigPath
            };
            StorageCli.testMain(validateArgs);

            assertFalse(errContent.toString("UTF-8").contains("Invalid hostname or port"));

            // test with invalid connection
            int fakePort = 9999;
            addStorageNode(cliConfigPath, helper.getHost(), fakePort, helper.getStorageAdminPort(), 0);

            String[] validateArgs2 = {
                    "validate",
                    "--cli-config-path", cliConfigPath
            };
            StorageCli.testMain(validateArgs2);

            assertTrue(errContent.toString("UTF-8").contains("Invalid hostname or port"));
            assertTrue(errContent.toString("UTF-8").contains(String.format("failed to connect: %s:%d", helper.getHost(), fakePort)));
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testMaxTransactionId() throws Exception {
        long sessionId = 123;
        int numPartitions = 3;
        IntegrationTestHelper helper = getIntegrationTestHelper(numPartitions);

        SslContext sslCtx = helper.getSslContext();
        int partitionId = new Random().nextInt(helper.getNumPartitions());

        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        helper.startZooKeeperServer();

        WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner();

        UUID key = helper.getClusterKey();

        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            String host = helper.getHost();
            int port = waltzStorage.port;
            int adminPort = waltzStorage.adminPort;

            // Create partitions on each storage node
            StorageAdminClient adminClient = helper.getStorageAdminClient(adminPort);
            adminClient.open();
            for (int i = 0; i < numPartitions; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }
            adminClient.close();

            // successful path
            String[] args0 = {
                    "max-transaction-id",
                    "--storage", host + ":" + adminPort,
                    "--storage-port", String.valueOf(port),
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args0);

            String expectedCmdOutput = "Max Transaction ID: " + -1;
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));

            // failure path
            String[] args1 = {
                    "max-transaction-id",
                    "--storage", "badhost.local:" + adminPort,
                    "--storage-port", String.valueOf(port),
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };
            StorageCli.testMain(args1);
            expectedCmdOutput = "Error: Failed to read max transaction ID for storage badhost.local:" + adminPort;
            assertTrue(errContent.toString("UTF-8").contains(expectedCmdOutput));

            // Load records into the source node
            ArrayList<Record> records = ClientUtil.makeRecords(0, 10);
            StorageClient sourceClient = new StorageClient(host, port, sslCtx, key, numPartitions);
            sourceClient.open();
            sourceClient.setLowWaterMark(sessionId, partitionId, -1L).get();
            sourceClient.appendRecords(sessionId, partitionId, records).get();
            sourceClient.setLowWaterMark(sessionId + 1, partitionId, records.size() - 1).get();

            // mark storage offline
            String[] args2 = {
                    "availability",
                    "--storage", host + ":" + adminPort,
                    "--partition", String.valueOf(partitionId),
                    "--online", String.valueOf(false),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args2);

            // successful path
            String[] args3 = {
                    "max-transaction-id",
                    "--storage", host + ":" + adminPort,
                    "--storage-port", String.valueOf(port),
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath,
                    "--offline"
            };

            StorageCli.testMain(args3);

            expectedCmdOutput = "Max Transaction ID: " + 9;
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));

            // failure path
            String[] args4 = {
                    "max-transaction-id",
                    "--storage", host + ":" + adminPort,
                    "--storage-port", String.valueOf(port),
                    "--partition", String.valueOf(partitionId),
                    "--cli-config-path", configFilePath
            };

            StorageCli.testMain(args4);

            expectedCmdOutput = "Error: Failed to read max transaction ID for storage " + host + ":" + adminPort;
            assertTrue(errContent.toString("UTF-8").contains(expectedCmdOutput));
        } finally {
            helper.closeAll();
        }
    }

    private void addStorageNode(String cliConfigPath, String host, int port, int adminPort, int i) {
        String[] addStorageArgs = {
                "add-storage-node",
                "-c", cliConfigPath,
                "-s", host + ":" + port,
                "-a", String.valueOf(adminPort),
                "-g", String.valueOf(0)
        };
        ZooKeeperCli.testMain(addStorageArgs);
    }

    private IntegrationTestHelper getIntegrationTestHelper(int numPartitions) throws Exception {
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/storage/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(numPartitions));
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        return new IntegrationTestHelper(properties);
    }
}
