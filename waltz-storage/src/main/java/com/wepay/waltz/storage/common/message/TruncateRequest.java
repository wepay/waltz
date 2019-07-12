package com.wepay.waltz.storage.common.message;

public class TruncateRequest extends StorageMessage {

    public final long transactionId;

    public TruncateRequest(long sessionId, long seqNum, int partitionId, long transactionId) {
        this(sessionId, seqNum, partitionId, transactionId, false);
    }

    public TruncateRequest(long sessionId, long seqNum, int partitionId, long transactionId, boolean usedByOfflineRecovery) {
        super(sessionId, seqNum, partitionId, usedByOfflineRecovery);

        this.transactionId = transactionId;
    }

    @Override
    public byte type() {
        return StorageMessageType.TRUNCATE_REQUEST;
    }
}
