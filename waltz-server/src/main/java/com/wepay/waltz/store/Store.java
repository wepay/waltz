package com.wepay.waltz.store;

import com.wepay.waltz.common.metadata.store.internal.ReplicaId;

import java.util.Set;

/**
 * This class handles the store.
 */
public interface Store {

    /**
     * Close the store
     */
    void close();

    /**
     * Returns the partition of the specified partition id.
     * The current generation number is also set.
     * @param partitionId the partition id
     * @param generation the generation number
     * @return the partition
     */
    StorePartition getPartition(int partitionId, int generation);

    /**
     * Return a set of ReplicaId that contains connection strings
     * for each partition.
     * @return connectString
     */
    Set<ReplicaId> getReplicaIds();

}
