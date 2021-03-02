package com.wepay.waltz.store.internal;

/**
 * This class handles connection factory for Replica.
 */
public interface ReplicaConnectionFactory {

    /**
     * Returns the {@link ReplicaConnection}.
     * @param partitionId The partition Id.
     * @param sessionId The session Id.
     * @return {@code ReplicaConnection}.
     * @throws Exception thrown if the Storage connection fails.
     */
    ReplicaConnection get(int partitionId, long sessionId) throws Exception;

    /**
     * Closes the replica connection factory.
     */
    void close();

}
