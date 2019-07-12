package com.wepay.waltz.storage.common.message;

public class RecordHeaderListRequest extends StorageMessage {

    public final long transactionId;
    public final int maxNumRecords;

    public RecordHeaderListRequest(long sessionId, long seqNum, int partitionId, long transactionId, int maxNumRecords) {
        super(sessionId, seqNum, partitionId);

        this.transactionId = transactionId;
        this.maxNumRecords = maxNumRecords;
    }

    @Override
    public byte type() {
        return StorageMessageType.RECORD_HEADER_LIST_REQUEST;
    }

}
