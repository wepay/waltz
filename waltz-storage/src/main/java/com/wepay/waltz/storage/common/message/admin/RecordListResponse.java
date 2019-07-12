package com.wepay.waltz.storage.common.message.admin;

import com.wepay.waltz.common.message.Record;

import java.util.ArrayList;

public class RecordListResponse extends AdminMessage {

    public final int partitionId;
    public final ArrayList<Record> records;

    public RecordListResponse(long seqNum, int partitionId, ArrayList<Record> records) {
        super(seqNum);

        this.partitionId = partitionId;
        this.records = records;
    }

    @Override
    public byte type() {
        return AdminMessageType.RECORD_LIST_RESPONSE;
    }

}
