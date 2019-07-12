package com.wepay.waltz.store.internal;

import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.internal.metadata.ReplicaId;

import java.util.ArrayList;

public interface RecoveryManager {

    void start(ArrayList<ReplicaSession> replicaSessions);

    void abort(Throwable reason);

    long start(ReplicaId replicaId, ReplicaConnection connection) throws RecoveryFailedException;

    void end(ReplicaId replicaId) throws RecoveryFailedException;

    long highWaterMark() throws RecoveryFailedException;

}
