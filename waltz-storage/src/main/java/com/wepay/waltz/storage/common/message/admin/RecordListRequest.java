package com.wepay.waltz.storage.common.message.admin;

public class RecordListRequest extends AdminMessage {

    public final int partitionId;
    public final long transactionId;
    public final int maxNumRecords;

    public RecordListRequest(long seqNum, int partitionId, long transactionId, int maxNumRecords) {
        super(seqNum);

        this.partitionId = partitionId;
        this.transactionId = transactionId;
        this.maxNumRecords = maxNumRecords;
    }

    @Override
    public byte type() {
        return AdminMessageType.RECORD_LIST_REQUEST;
    }

}
