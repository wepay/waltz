package com.wepay.waltz.store.internal;

import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.internal.metadata.ReplicaId;

import java.util.ArrayList;

/**
 * This class handles recovery.
 */
public interface RecoveryManager {

    /**
     * Starts the recovery.
     * @param replicaSessions List of {@link ReplicaSession}.
     */
    void start(ArrayList<ReplicaSession> replicaSessions);

    /**
     * This method gets called if the recovery aborted.
     * @param reason The reason for abort.
     */
    void abort(Throwable reason);

    /**
     * Starts recovery for the given replica.
     * @param replicaId The replica Id.
     * @param connection The {@link ReplicaConnection}.
     * @return the high-water mark.
     * @throws RecoveryFailedException thrown if the recovery failed.
     */
    long start(ReplicaId replicaId, ReplicaConnection connection) throws RecoveryFailedException;

    /**
     * This method handles the recovery completion.
     * @param replicaId The replicaId.
     * @throws RecoveryFailedException thrown if the recovery fails.
     */
    void end(ReplicaId replicaId) throws RecoveryFailedException;

    /**
     * Returns the high-water mark.
     * @return the high-water mark.
     * @throws RecoveryFailedException thrown if the recovery fails.
     */
    long highWaterMark() throws RecoveryFailedException;

}
