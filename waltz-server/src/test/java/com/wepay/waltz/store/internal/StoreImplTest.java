package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.metadata.PartitionMetadata;
import com.wepay.waltz.common.metadata.PartitionMetadataSerializer;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.common.metadata.ReplicaState;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.metadata.StoreParams;
import com.wepay.waltz.storage.WaltzStorage;
import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StoreImplTest {
    private static final int NUM_REPLICAS = 3;
    private static final int CLUSTER_NUM_PARTITIONS = 3;

    @Test
    public void testReplicaAssignmentChangeEffects() throws Exception {
        IntegrationTestHelper helper = null;
        ZooKeeperClient zkClient = null;
        try {
            String rootPath = "/storeimpl/test";
            Properties properties = new Properties();
            properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, rootPath);

            properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(CLUSTER_NUM_PARTITIONS));
            properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
            properties.setProperty(IntegrationTestHelper.Config.NUM_STORAGES, String.valueOf(NUM_REPLICAS));

            helper = new IntegrationTestHelper(properties);
            helper.startZooKeeperServer();

            List<String> storageConnectStrings = new ArrayList<>();
            List<Integer> adminPorts = new ArrayList<>();
            for (int s = 0; s < NUM_REPLICAS; s++) {
                WaltzStorageRunner storageRunner = helper.getWaltzStorageRunner(s);
                storageRunner.startAsync();
                WaltzStorage storage = storageRunner.awaitStart();
                adminPorts.add(storage.adminPort);
                storageConnectStrings.add(helper.getStorageConnectString(s));
            }

            // Wait for partition creation on storage nodes.
            Thread.sleep(500);

            for (int adminPort : adminPorts) {
                helper.setWaltzStorageAssignmentWithPort(adminPort, true);
            }

            helper.startWaltzServer(true);

            // Wait for recovery and replica states updates to ZK.
            Thread.sleep(500);

            ZNode root = new ZNode(rootPath);
            zkClient = new ZooKeeperClientImpl(helper.getZkConnectString(), helper.getZkSessionTimeout());

            Map<Integer, Long> partitionSessionIds = new HashMap<>();
            Map<Integer, List<String>> partitionReplicas = new HashMap<>();
            populateReplicaStateInfo(zkClient, root, partitionSessionIds, partitionReplicas);

            assertEquals(CLUSTER_NUM_PARTITIONS, partitionSessionIds.size());
            assertEquals(CLUSTER_NUM_PARTITIONS, partitionReplicas.size());
            partitionReplicas.forEach((partitionId, replicaConnectStrings) ->
                assertEquals(new HashSet<>(storageConnectStrings), new HashSet<>(replicaConnectStrings))
            );

            // Remove all partitions from the first storage node.
            StoreMetadata storeMetadata = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME));
            List<Integer> partitions = IntStream.range(0, CLUSTER_NUM_PARTITIONS).boxed().collect(Collectors.toList());
            storeMetadata.removePartitions(partitions, helper.getStorageConnectString(0));

            // Wait for recovery and replica states updates to ZK.
            Thread.sleep(1000);

            Map<Integer, Long> newPartitionSessionIds = new HashMap<>();
            Map<Integer, List<String>> newPartitionReplicas = new HashMap<>();
            populateReplicaStateInfo(zkClient, root, newPartitionSessionIds, newPartitionReplicas);

            assertEquals(CLUSTER_NUM_PARTITIONS, partitionSessionIds.size());
            assertEquals(CLUSTER_NUM_PARTITIONS, partitionReplicas.size());
            newPartitionReplicas.forEach((partitionId, replicaConnectStrings) -> {
                assertEquals(NUM_REPLICAS - 1, replicaConnectStrings.size());
                assertTrue(storageConnectStrings.containsAll(replicaConnectStrings));
            });
            newPartitionSessionIds.forEach((partitionId, sessionId) ->
                assertTrue(sessionId > partitionSessionIds.get(partitionId))
            );
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
            if (helper != null) {
                helper.closeAll();
            }
        }
    }

    private void populateReplicaStateInfo(ZooKeeperClient zkClient,
                                          ZNode root,
                                          Map<Integer, Long> partitionSessionIds,
                                          Map<Integer, List<String>> partitionReplicas) throws Exception {
        ZNode storeRoot = new ZNode(root, StoreMetadata.STORE_ZNODE_NAME);
        StoreMetadata storeMetadata = new StoreMetadata(zkClient, storeRoot);
        ZNode partitionRoot = new ZNode(storeRoot, StoreMetadata.PARTITION_ZNODE_NAME);

        StoreParams storeParams = storeMetadata.getStoreParams();

        for (int id = 0; id < storeParams.numPartitions; id++) {
            ZNode znode = new ZNode(partitionRoot, Integer.toString(id));
            PartitionMetadata partitionMetadata =
                zkClient.getData(znode, PartitionMetadataSerializer.INSTANCE).value;
            Map<ReplicaId, ReplicaState> replicaStates = partitionMetadata.replicaStates;
            partitionSessionIds.putIfAbsent(id, partitionMetadata.sessionId);
            partitionReplicas.put(id, replicaStates.keySet().stream().map(replicaId ->
                replicaId.storageNodeConnectString).collect(Collectors.toList()));
        }
    }
}
