package com.wepay.waltz.store.internal;

import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.exception.ReplicaConnectionFactoryClosedException;

/**
 * This class handles connection factory for Replica.
 */
public interface ReplicaConnectionFactory {

    /**
     * Returns the {@link ReplicaConnection}.
     * @param partitionId The partition Id.
     * @param sessionId The session Id.
     * @return {@code ReplicaConnection}.
     * @throws StorageRpcException thrown if Storage connection fails.
     * @throws ReplicaConnectionFactoryClosedException thrown if the replica connection is closed.
     */
    ReplicaConnection get(int partitionId, long sessionId) throws StorageRpcException, ReplicaConnectionFactoryClosedException;

    /**
     * Closes the replica connection factory.
     */
    void close();

}
