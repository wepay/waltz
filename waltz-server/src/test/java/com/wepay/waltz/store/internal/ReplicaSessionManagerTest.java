package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.metadata.store.internal.ReplicaAssignments;
import com.wepay.zktools.zookeeper.NodeData;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class ReplicaSessionManagerTest {

    @Test
    public void testUpdateReplicaSessionManager() {
        String storage1 = "fakehost:6000";
        String storage2 = "fakehost:6001";
        int[] oldPartitions1 = {};
        int[] oldPartitions2 = {0};
        int[] newPartitions1 = {1};
        int[] newPartitions2 = {0, 1};

        Map<String, int[]> oldReplicas = new HashMap<>();
        oldReplicas.put(storage1, oldPartitions1);
        oldReplicas.put(storage2, oldPartitions2);
        ReplicaAssignments oldAssignments = new ReplicaAssignments(oldReplicas);

        Map<String, int[]> newReplicas = new HashMap<>();
        // update partitions assignment for storage1
        newReplicas.put(storage1, newPartitions1);
        newReplicas.put(storage2, newPartitions2);
        ReplicaAssignments newAssignments = new ReplicaAssignments(newReplicas);

        NodeData<ReplicaAssignments> nodeData = new NodeData<>(newAssignments, Mockito.mock(Stat.class));
        ConnectionConfig config = Mockito.mock(ConnectionConfig.class);
        ReplicaSessionManager manager = new ReplicaSessionManager(oldAssignments, config);

        Assert.assertEquals(1, manager.getReplicaIds().size());
        Assert.assertEquals(1, manager.getConnectionFactories().size());

        manager.updateReplicaSessionManager(nodeData);

        Assert.assertEquals(3, manager.getReplicaIds().size());
        Assert.assertEquals(2, manager.getConnectionFactories().size());
    }

}
