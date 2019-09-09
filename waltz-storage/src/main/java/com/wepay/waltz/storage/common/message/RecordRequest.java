package com.wepay.waltz.storage.common.message;

public class RecordRequest extends StorageMessage {

    public final long transactionId;

    public RecordRequest(long sessionId, long seqNum, int partitionId, long transactionId) {
        super(sessionId, seqNum, partitionId);

        this.transactionId = transactionId;
    }

    @Override
    public byte type() {
        return StorageMessageType.RECORD_REQUEST;
    }

}
