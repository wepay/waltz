package com.wepay.waltz.storage.common.message;

public class MaxTransactionIdRequest extends StorageMessage {

    public MaxTransactionIdRequest(long sessionId, long seqNum, int partitionId) {
        this(sessionId, seqNum, partitionId, false);
    }

    public MaxTransactionIdRequest(long sessionId, long seqNum, int partitionId, boolean usedByOfflineRecovery) {
        super(sessionId, seqNum, partitionId, usedByOfflineRecovery);
    }

    @Override
    public byte type() {
        return StorageMessageType.MAX_TRANSACTION_ID_REQUEST;
    }
}
