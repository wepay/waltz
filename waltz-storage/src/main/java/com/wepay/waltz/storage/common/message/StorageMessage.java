package com.wepay.waltz.storage.common.message;

public abstract class StorageMessage extends SequenceMessage {

    public final long sessionId;
    public final int partitionId;
    public final boolean usedByOfflineRecovery;

    StorageMessage(long sessionId, long seqNum, int partitionId) {
        this(sessionId, seqNum, partitionId, false);
    }

    StorageMessage(long sessionId, long seqNum, int partitionId, boolean usedByOfflineRecovery) {
        super(seqNum);
        this.sessionId = sessionId;
        this.partitionId = partitionId;
        this.usedByOfflineRecovery = usedByOfflineRecovery;
    }

}
