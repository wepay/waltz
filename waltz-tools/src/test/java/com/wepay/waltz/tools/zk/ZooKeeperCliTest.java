package com.wepay.waltz.tools.zk;

import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.store.internal.metadata.GroupDescriptor;
import com.wepay.waltz.store.internal.metadata.GroupDescriptorSerializer;
import com.wepay.waltz.store.internal.metadata.PartitionMetadata;
import com.wepay.waltz.store.internal.metadata.PartitionMetadataSerializer;
import com.wepay.waltz.store.internal.metadata.ReplicaAssignments;
import com.wepay.waltz.store.internal.metadata.ReplicaAssignmentsSerializer;
import com.wepay.waltz.store.internal.metadata.ReplicaId;
import com.wepay.waltz.store.internal.metadata.ReplicaState;
import com.wepay.waltz.store.internal.metadata.StoreMetadata;
import com.wepay.waltz.store.internal.metadata.StoreParams;
import com.wepay.waltz.store.internal.metadata.StoreParamsSerializer;
import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.ZooKeeperServerRunner;
import com.wepay.waltz.tools.CliConfig;
import com.wepay.zktools.clustermgr.internal.ClusterParams;
import com.wepay.zktools.clustermgr.internal.ClusterParamsSerializer;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public final class ZooKeeperCliTest {

    private static final int NUM_PARTITIONS = 3;
    private static final int STORAGE_GROUP_0 = 0;
    private static final int STORAGE_GROUP_1 = 1;
    private static final int ZK_SESSION_TIMEOUT = 30000;

    private static final String CLUSTER_ROOT = "/test/root";
    private static final String CLUSTER_NAME = "tool test";
    private static final String DIR_NAME = "zkCliTest";
    private static final String CONFIG_FILE_NAME = "test-config.yml";

    private static final Map<String, Integer> STORAGE_GROUP_MAP = Utils.map(
            "fakehost0:6000", STORAGE_GROUP_1,
            "fakehost1:6001", STORAGE_GROUP_1
    );

    private static final Map<String, Integer> STORAGE_CONNECTION_MAP = Utils.map(
            "fakehost0:6000", 6100,
            "fakehost1:6001", 6101
    );

    Properties createProperties(String connectString, String znodePath) {
        Properties properties = new Properties();
        properties.setProperty(CliConfig.ZOOKEEPER_CONNECT_STRING, connectString);
        properties.setProperty(CliConfig.CLUSTER_ROOT, znodePath);

        return properties;
    }

    @Test
    public void testCreate() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            // test Create command
            String[] createCommandArgs = new String[]{
                    "create",
                    "--name", CLUSTER_NAME,
                    "--partitions", Integer.toString(NUM_PARTITIONS),
                    "--cli-config-path", configFilePath
            };
            ZooKeeperCli.testMain(createCommandArgs);

            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);

            ZNode root = new ZNode(CLUSTER_ROOT);

            ClusterParamsSerializer serializer = new ClusterParamsSerializer();
            NodeData<ClusterParams> nodeData = zkClient.getData(root, serializer);
            assertNotNull(nodeData.value);
            assertEquals(CLUSTER_NAME, nodeData.value.name);
            assertEquals(NUM_PARTITIONS, nodeData.value.numPartitions);

            ZNode storeRoot = new ZNode(root, StoreMetadata.STORE_ZNODE_NAME);
            NodeData<StoreParams> storeRootData = zkClient.getData(storeRoot, StoreParamsSerializer.INSTANCE);

            assertNotNull(storeRootData.value);
            assertNotNull(storeRootData.value.key);
            assertEquals(NUM_PARTITIONS, storeRootData.value.numPartitions);

            ZNode groupNode = new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME);
            NodeData<GroupDescriptor> groupDescriptorData = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE);
            GroupDescriptor groupDescriptor = groupDescriptorData.value;

            assertNotNull(groupDescriptor);

            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            NodeData<ReplicaAssignments> assignmentsData = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE);
            ReplicaAssignments replicaAssignments = assignmentsData.value;

            assertNotNull(replicaAssignments);
            Map<String, int[]> replicas = assignmentsData.value.replicas;
            assertEquals(0, replicas.size());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testList() throws Exception {
        // set up streams
        PrintStream originalOut = System.out;
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent, false, "UTF-8"));

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int partitionId = 0, generation = 5, sessionId = 10, numReplicas = 2;
            String connectStringTemplate = "fakehost%d:600%d";
            HashMap<ReplicaId, ReplicaState> replicaStates = new HashMap<>();
            ZNode partitionRoot = new ZNode(storeRoot, StoreMetadata.PARTITION_ZNODE_NAME);
            ZNode znode = new ZNode(partitionRoot, Integer.toString(partitionId));
            for (int i = 0; i < numReplicas; i++) {
                ReplicaId replicaId = new ReplicaId(partitionId, String.format(connectStringTemplate, i, i));
                replicaStates.put(replicaId, new ReplicaState(replicaId, -1L, ReplicaState.UNRESOLVED));
            }

            zkClient.create(
                    znode,
                    new PartitionMetadata(generation, sessionId, replicaStates),
                    PartitionMetadataSerializer.INSTANCE,
                    CreateMode.PERSISTENT
            );

            String[] showCommandArgs = {
                    "list",
                    "--cli-config-path", configFilePath
            };
            ZooKeeperCli.testMain(showCommandArgs);

            String expectedCmdOutput = "store [/test/root/store] replica and group assignments:\n"
                    + "    fakehost0:6000 = [0, 1, 2], GroupId: 1\n"
                    + "    fakehost1:6001 = [0, 1, 2], GroupId: 1\n"
                    + "store [/test/root/store/partition/0] replica states:\n"
                    + "  ReplicaId(0,fakehost0:6000), SessionId: -1, closingHighWaterMark: UNRESOLVED\n"
                    + "  ReplicaId(0,fakehost1:6001), SessionId: -1, closingHighWaterMark: UNRESOLVED\n"
                    + "store [/test/root/store/partition/1] replica states:\n"
                    + "  no node found\n"
                    + "store [/test/root/store/partition/2] replica states:\n"
                    + "  no node found\n";

            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));

        } finally {
            // restore streams
            System.setOut(originalOut);
            outContent.close();

            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAddStorageNode() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode groupNode = new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int prevGroupSize = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE).value.groups.size();
            int prevReplicasSize = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.size();

            String storageNodeToAdd = "fakehost0:8000";
            int storageAdminPort = 8100;
            addStorageNode(storageNodeToAdd, storageAdminPort, STORAGE_GROUP_1, configFilePath);

            Map<String, Integer> groups = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE).value.groups;
            Map<String, int[]> replicas = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas;

            assertEquals(prevGroupSize + 1, groups.size());
            assertEquals(prevReplicasSize + 1, replicas.size());
            assertTrue(groups.containsKey(storageNodeToAdd));
            assertTrue(replicas.containsKey(storageNodeToAdd));

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAddExistingStorageNodeShouldFail() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode groupNode = new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int prevGroupSize = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE).value.groups.size();

            // add an existing storage node
            String storageNodeToAdd = "fakehost0:6000";
            int storageAdminPort = 6100;
            addStorageNode(storageNodeToAdd, storageAdminPort, STORAGE_GROUP_1, configFilePath);

            NodeData<GroupDescriptor> groupDescriptorData = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE);
            Map<String, Integer> groups = groupDescriptorData.value.groups;

            // number of partitions remain the same
            assertEquals(prevGroupSize, groups.size());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testRemoveStorageNode() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode groupNode = new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, 0, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int prevGroupSize = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE).value.groups.size();
            int prevReplicasSize = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.size();

            // remove the empty storage node
            String storageNodeToRemove = "fakehost0:6000";
            removeStorageNode(storageNodeToRemove, configFilePath);

            Map<String, Integer> groups = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE).value.groups;
            Map<String, int[]> replicas = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas;

            assertEquals(prevReplicasSize - 1, replicas.size());
            assertEquals(prevGroupSize - 1, groups.size());
            assertTrue(!groups.containsKey(storageNodeToRemove));
            assertTrue(!replicas.containsKey(storageNodeToRemove));

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testRemoveNonEmptyStorageNodeShouldFail() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode groupNode = new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int prevGroupSize = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE).value.groups.size();

            // remove an non-empty storage node
            String storageNodeToRemove = "fakehost0:6000";
            removeStorageNode(storageNodeToRemove, configFilePath);

            NodeData<GroupDescriptor> nodeData = zkClient.getData(groupNode, GroupDescriptorSerializer.INSTANCE);
            Map<String, Integer> groups = nodeData.value.groups;

            // number of storage nodes remain the same
            assertEquals(prevGroupSize, groups.size());

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAssignPartition() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            // add a new storage
            String storageNodeToAdd = "fakehost2:6002";
            int storageAdminPort = 6102;
            addStorageNode(storageNodeToAdd, storageAdminPort, STORAGE_GROUP_1, configFilePath);

            String storageNodeAssignTo = storageNodeToAdd;
            int partitionToAssign = 0;
            int[] prevPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeAssignTo);
            int prevPartitionSize = prevPartitions.length;

            // add a new partition
            addPartition(partitionToAssign, storageNodeAssignTo, configFilePath);

            int[] currPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeAssignTo);
            int currPartitionSize = currPartitions.length;

            assertEquals(prevPartitionSize + 1, currPartitionSize);
            assertTrue(Arrays.stream(currPartitions).boxed().collect(Collectors.toSet()).contains(partitionToAssign));

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAssignDuplicatePartitionShouldFail() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int partitionToAssign = 0;
            String storageNodeAssignTo = "fakehost0:6000";

            int[] prevPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeAssignTo);
            int prevPartitionSize = prevPartitions.length;

            // add a duplicate partition
            addPartition(partitionToAssign, storageNodeAssignTo, configFilePath);

            int[] currPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeAssignTo);
            int currPartitionSize = currPartitions.length;

            // number of partitions remain the same
            assertEquals(prevPartitionSize, currPartitionSize);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAssignPartitionToNonExistingStorageShouldFail() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int partitionToAssign = 0;
            String storageNodeAssignTo = "fakehost0:7000";
            addPartition(partitionToAssign, storageNodeAssignTo, configFilePath);

            int[] currPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeAssignTo);

            // replicas does not contain such storage node
            assertNull(currPartitions);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testUnassignPartition() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            int partitionToRemove = 0;
            String storageNodeRemoveFrom = "fakehost0:6000";

            int[] prevPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeRemoveFrom);
            int prevPartitionSize = prevPartitions.length;

            removePartition(partitionToRemove, storageNodeRemoveFrom, configFilePath);

            int[] currPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeRemoveFrom);
            int currPartitionSize = currPartitions.length;

            assertEquals(prevPartitionSize - 1, currPartitionSize);
            assertTrue(!Arrays.stream(currPartitions).boxed().collect(Collectors.toSet()).contains(partitionToRemove));

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testUnassignNonExistingPartitionShouldFail() throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, STORAGE_GROUP_MAP, STORAGE_CONNECTION_MAP);

            // non-existing partition
            int partitionToRemove = 3;
            String storageNodeRemoveFrom = "fakehost0:6000";

            int[] prevPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeRemoveFrom);
            int prevPartitionSize = prevPartitions.length;

            // remove non-existing partition
            removePartition(partitionToRemove, storageNodeRemoveFrom, configFilePath);

            int[] currPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeRemoveFrom);
            int currPartitionSize = currPartitions.length;

            // number of partitions remain the same
            assertEquals(prevPartitionSize, currPartitionSize);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testUnassignLastCopyOfSuchPartitionInGroupShouldFail() throws Exception {
        Map<String, Integer> storageGroupMap = Utils.map(
                "fakehost0:6000", STORAGE_GROUP_1
        );
        Map<String, Integer> storageConnectionMap = Utils.map(
                "fakehost0:6000", 6100
        );
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, NUM_PARTITIONS, storageGroupMap, storageConnectionMap);

            int partitionToRemove = 0;
            String storageNodeRemoveFrom = "fakehost0:6000";

            int[] prevPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeRemoveFrom);
            int prevPartitionSize = prevPartitions.length;

            // remove the last copy of that partition in group
            removePartition(partitionToRemove, storageNodeRemoveFrom, configFilePath);

            int[] currPartitions = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas.get(storageNodeRemoveFrom);
            int currPartitionSize = currPartitions.length;

            // number of partitions remain the same
            assertEquals(prevPartitionSize, currPartitionSize);

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAutoAssign() throws Exception {
        int numPartitions = 12;
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, numPartitions);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, numPartitions);


            // add storage nodes to group
            List<String> storageNodeList = Utils.list("fakehost0:6000", "fakehost1:6001", "fakehost2:6002");
            List<Integer> storageAdminPortList = Utils.list(6100, 6101, 6102);
            for (int i = 0; i < storageNodeList.size(); i++) {
                addStorageNode(storageNodeList.get(i), storageAdminPortList.get(i), STORAGE_GROUP_1, configFilePath);
            }

            autoAssignPartitions(STORAGE_GROUP_1, configFilePath);

            // check if partition evenly distributed among storage
            Map<String, int[]> newReplicas = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas;
            for (String storage : storageNodeList) {
                assertEquals(4, newReplicas.get(storage).length); // numPartitions / numStorage = 4
            }

            // check if the group contains full partitions
            assertEquals(numPartitions, getNumPartitionsInGroup(storageNodeList, newReplicas));
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAutoAssignMultipleGroup() throws Exception {
        int numPartitions = 12;
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, numPartitions);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, numPartitions);

            List<String> storageNodeList = Utils.list("fakehost0:6000", "fakehost1:6001", "fakehost2:6002",
                    "fakehost3:6003", "fakehost4:6004", "fakehost5:6005");
            List<Integer> storageAdminPortList = Utils.list(6100, 6101, 6102, 6103, 6104, 6105);
            // add storage nodes to group 0
            for (int i = 0; i < 3; i++) {
                String storage = storageNodeList.get(i);
                int storageAdminPort = storageAdminPortList.get(i);
                addStorageNode(storage, storageAdminPort, STORAGE_GROUP_0, configFilePath);
            }

            // add storage nodes to group 1
            for (int i = 3; i < 6; i++) {
                String storage = storageNodeList.get(i);
                int storageAdminPort = storageAdminPortList.get(i);
                addStorageNode(storage, storageAdminPort, STORAGE_GROUP_1, configFilePath);
            }

            autoAssignPartitions(STORAGE_GROUP_0, configFilePath);
            autoAssignPartitions(STORAGE_GROUP_1, configFilePath);

            Map<String, int[]> newReplicas = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas;

            // check if partition evenly distributed among storage
            assertEquals(6, newReplicas.size());
            for (String storage : storageNodeList) {
                assertEquals(4, newReplicas.get(storage).length);
            }

            // check if the group contains full partitions
            assertEquals(numPartitions, getNumPartitionsInGroup(storageNodeList, newReplicas));
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testAutoAssignStorageNodeWithAssignmentShouldFail() throws Exception {
        int numPartitions = 12;
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        String connectString = zooKeeperServerRunner.start();
        String configFilePath = IntegrationTestHelper.createYamlConfigFile(DIR_NAME, CONFIG_FILE_NAME, createProperties(connectString, CLUSTER_ROOT));

        try {
            ZNode clusterRootZNode = new ZNode(CLUSTER_ROOT);
            ZNode storeRoot = new ZNode(clusterRootZNode, StoreMetadata.STORE_ZNODE_NAME);
            ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, ZK_SESSION_TIMEOUT);
            ZooKeeperCli.Create.createCluster(zkClient, clusterRootZNode, CLUSTER_NAME, numPartitions);
            ZooKeeperCli.Create.createStores(zkClient, clusterRootZNode, numPartitions);

            // add storage nodes to group
            List<String> storageNodeList = Utils.list("fakehost0:6000", "fakehost1:6001", "fakehost2:6002");
            List<Integer> storageAdminPortList = Utils.list(6100, 6101, 6102);
            for (int i = 0; i < storageNodeList.size(); i++) {
                addStorageNode(storageNodeList.get(i), storageAdminPortList.get(i), STORAGE_GROUP_1, configFilePath);
            }

            // assign a partition to one of the storage node in group
            int partitionToAssign = 0;
            String storageNodeToAssignTo = storageNodeList.get(0);
            addPartition(partitionToAssign, storageNodeToAssignTo, configFilePath);

            autoAssignPartitions(STORAGE_GROUP_1, configFilePath);

            // check if the group only contains one partition
            Map<String, int[]> newReplicas = zkClient.getData(assignmentNode, ReplicaAssignmentsSerializer.INSTANCE).value.replicas;
            assertEquals(1, getNumPartitionsInGroup(storageNodeList, newReplicas));
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private void addStorageNode(String storageNodeToAdd, int adminPort, int groupId, String configFilePath) {
        String[] addCommandArgs = {
                "add-storage-node",
                "-c", configFilePath,
                "-s", storageNodeToAdd,
                "-a", String.valueOf(adminPort),
                "-g", String.valueOf(groupId)
        };

        ZooKeeperCli.testMain(addCommandArgs);
    }

    private void removeStorageNode(String storageNodeToRemove, String configFilePath) {
        String[] removeCommandArgs = {
                "remove-storage-node",
                "-c", configFilePath,
                "-s", storageNodeToRemove
        };

        ZooKeeperCli.testMain(removeCommandArgs);
    }

    private void addPartition(int partitionToAssign, String storageNodeAssignTo, String configFilePath) {
        String[] assignCommandArgs = {
                "assign-partition",
                "-c", configFilePath,
                "-p", String.valueOf(partitionToAssign),
                "-s", storageNodeAssignTo
        };

        ZooKeeperCli.testMain(assignCommandArgs);
    }

    private void removePartition(int partitionToRemove, String storageNodeRemoveFrom, String configFilePath) {
        String[] unassignCommandArgs = {
                "unassign-partition",
                "-c", configFilePath,
                "-p", String.valueOf(partitionToRemove),
                "-s", storageNodeRemoveFrom
        };

        ZooKeeperCli.testMain(unassignCommandArgs);
    }

    private void autoAssignPartitions(int groupId, String configFilePath) {
        String[] autoAssignCommandArgs = {
                "auto-assign",
                "-c", configFilePath,
                "-g", String.valueOf(groupId)
        };

        ZooKeeperCli.testMain(autoAssignCommandArgs);
    }

    private int getNumPartitionsInGroup(List<String> storageNodeList, Map<String, int[]> replicas) {
        Set<Integer> partitionSet = new HashSet<>();
        for (String storage : storageNodeList) {
            int[] partitions = replicas.get(storage);
            if (partitions != null && partitions.length > 0) {
                partitionSet.addAll(Arrays.stream(partitions).boxed().collect(Collectors.toSet()));
            }
        }
        return partitionSet.size();
    }
}
