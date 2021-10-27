package com.wepay.waltz.tools.server;

import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.SeparateClassLoaderJUnitRunner;
import com.wepay.waltz.test.util.SslSetup;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.clustermgr.internal.PartitionAssignment;
import com.wepay.zktools.clustermgr.internal.ServerDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

@RunWith(SeparateClassLoaderJUnitRunner.class)
public final class ServerCliTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    private static final String DIR_NAME = "serverCliTest";
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
//        int numStorages = 3;
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/server/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "3");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");

        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        int jettyPort = helper.getServerJettyPort();

        try {
            helper.startZooKeeperServer();

            helper.startWaltzServer(true);

            String[] args = {
                    "list",
                    "--server",
                    helper.getHost() + ":" + jettyPort
            };
            ServerCli.testMain(args);
            String expectedCmdOutput = "There are " + helper.getNumPartitions() + " partitions for current server";
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));
        } finally {
            helper.closeAll();
        }
    }

    @Test
    public void testAddAndRemovePreferredPartition() throws Exception {
        int numPartitions = 8;
        int numServers = 2;
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/server/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(numPartitions));
        properties.setProperty(IntegrationTestHelper.Config.NUM_SERVERS, String.valueOf(numServers));
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");

        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        Properties cfgProperties = createProperties(helper.getZkConnectString(), helper.getZnodePath(), helper.getSslSetup());
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, cfgProperties);

        String partitionIdBatch = "3,6-7";
        Set<Integer> partitionIds = Stream.of(3, 6, 7).collect(Collectors.toCollection(HashSet::new));
        Set<ServerDescriptor> serverDescriptors = null;
        PartitionAssignment partitionAssignment = null;
        try {
            helper.startZooKeeperServer();

            // Start Server 1
            WaltzServerRunner serverRunner1 = helper.getWaltzServerRunner(0);
            serverRunner1.startAsync();
            serverRunner1.awaitStart();
            int serverPort1 = helper.getServerPort(0);
            String serverEndpoint1 = helper.getHost() + ":" + serverPort1;

            // Add partition P3 as preferred partition to server 1
            String[] args1 = {
                "add-preferred-partition",
                "--server", serverEndpoint1,
                "--partition", partitionIdBatch,
                "--cli-config-path", configFilePath
            };
            ServerCli.testMain(args1);

            // Verify preferred partitions P3,P6,P7 are added to the server descriptor of server 1
            serverDescriptors = helper.getWaltzServerRunner().getServerDescriptors();
            for (ServerDescriptor serverDescriptor : serverDescriptors) {
                if (serverDescriptor.endpoint.toString().equals(serverEndpoint1)) {
                    assertTrue(serverDescriptor.partitions.containsAll(partitionIds));

                    // Verify partitions P3,P6,P7 are assigned to server 1
                    int partitionMatchCounter = 0;
                    partitionAssignment = helper.getWaltzServerRunner().getPartitionAssignment();
                    List<PartitionInfo> partitionInfoList =
                        partitionAssignment.partitionsFor(serverDescriptor.serverId);
                    for (PartitionInfo partitionInfo : partitionInfoList) {
                        if (partitionIds.contains(partitionInfo.partitionId)) {
                            partitionMatchCounter += 1;
                        }
                    }
                    assertTrue(partitionMatchCounter == partitionIds.size());
                }
            }

            // Start Server 2
            WaltzServerRunner serverRunner2 = helper.getWaltzServerRunner(1);
            serverRunner2.startAsync();
            serverRunner2.awaitStart();
            int serverPort2 = helper.getServerPort(1);
            String serverEndpoint2 = helper.getHost() + ":" + serverPort2;

            // Add partitions P3,P6,P7 as preferred partition to server 2
            String[] args2 = {
                "add-preferred-partition",
                "--server", serverEndpoint2,
                "--partition", partitionIdBatch,
                "--cli-config-path", configFilePath
            };
            ServerCli.testMain(args2);

            // Verify preferred partition P3,P6,P7 is added to the server descriptor of server 2
            serverDescriptors = helper.getWaltzServerRunner(1).getServerDescriptors();
            for (ServerDescriptor serverDescriptor : serverDescriptors) {
                if (serverDescriptor.endpoint.toString().equals(serverEndpoint2)) {
                    assertTrue(serverDescriptor.partitions.containsAll(partitionIds));

                    // Verify partition P3,P6,P7 is not assigned to server 2
                    /**
                     * Note: Because P3,P6,P7 are also assigned as a preferred partition to Server 1 which means that as
                     * the preferred partitions are assigned sequentially, P3,P6,P7 are still assigned to Server 1 even
                     * though P3,P6,P7 are also a preferred partition to Server 2.
                     */
                    Boolean partitionMatch = false;
                    partitionAssignment = helper.getWaltzServerRunner(1).getPartitionAssignment();
                    List<PartitionInfo> partitionInfoList =
                        partitionAssignment.partitionsFor(serverDescriptor.serverId);
                    for (PartitionInfo partitionInfo : partitionInfoList) {
                        if (partitionIds.contains(partitionInfo.partitionId)) {
                            partitionMatch = true;
                        }
                    }
                    assertFalse(partitionMatch);
                }
            }

            // Remove partitions P3,P6,P7 as preferred partition from server 1
            String[] args3 = {
                "remove-preferred-partition",
                "--server", serverEndpoint1,
                "--partition", partitionIdBatch,
                "--cli-config-path", configFilePath
            };
            ServerCli.testMain(args3);

            // Verify preferred partitions P3,P6,P7 are removed from the server descriptor of Server 1
            serverDescriptors = helper.getWaltzServerRunner().getServerDescriptors();
            for (ServerDescriptor serverDescriptor : serverDescriptors) {
                if (serverDescriptor.endpoint.toString().equals(serverEndpoint1)) {
                    partitionIds.forEach(partitionId -> assertFalse(serverDescriptor.partitions.contains(partitionId)));
                }

                if (serverDescriptor.endpoint.toString().equals(serverEndpoint2)) {
                    assertTrue(serverDescriptor.partitions.containsAll(partitionIds));

                    // Verify partitions P3,P6,P7 are assigned to server 2
                    int partitionMatchCounter = 0;
                    partitionAssignment = helper.getWaltzServerRunner().getPartitionAssignment();
                    List<PartitionInfo> partitionInfoList =
                        partitionAssignment.partitionsFor(serverDescriptor.serverId);
                    for (PartitionInfo partitionInfo : partitionInfoList) {
                        if (partitionIds.contains(partitionInfo.partitionId)) {
                            partitionMatchCounter += 1;
                        }
                    }
                    assertTrue(partitionMatchCounter == partitionIds.size());
                }
            }
        } finally {
            helper.closeAll();
        }
    }

}
