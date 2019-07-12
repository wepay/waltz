package com.wepay.waltz.test.mock;

import com.wepay.waltz.store.Store;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.internal.metadata.ReplicaId;

import java.util.HashMap;
import java.util.Set;

public class MockStore implements Store {

    private final HashMap<Integer, StorePartition> partitions = new HashMap<>();

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public StorePartition getPartition(int partitionId, int generation) {
        synchronized (partitions) {
            StorePartition partition = partitions.get(partitionId);
            if (partition == null) {
                partition = new MockStorePartition(partitionId);
                partitions.put(partitionId, partition);
            }
            return partition;
        }
    }

    public Set<ReplicaId> getReplicaIds() {
        throw new UnsupportedOperationException();
    }

}
