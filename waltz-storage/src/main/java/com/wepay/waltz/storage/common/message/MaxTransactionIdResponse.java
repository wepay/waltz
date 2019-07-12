package com.wepay.waltz.storage.common.message;

public class MaxTransactionIdResponse extends StorageMessage {

    public final long transactionId;

    public MaxTransactionIdResponse(long sessionId, long seqNum, int partitionId, long transactionId) {
        super(sessionId, seqNum, partitionId);

        this.transactionId = transactionId;
    }

    @Override
    public byte type() {
        return StorageMessageType.MAX_TRANSACTION_ID_RESPONSE;
    }

}
