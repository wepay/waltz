package com.wepay.waltz.storage.common.message;

import com.wepay.waltz.common.message.Record;

import java.util.ArrayList;

public class RecordListResponse extends StorageMessage {

    public final ArrayList<Record> records;

    public RecordListResponse(long sessionId, long seqNum, int partitionId, ArrayList<Record> records) {
        super(sessionId, seqNum, partitionId);

        this.records = records;
    }

    @Override
    public byte type() {
        return StorageMessageType.RECORD_LIST_RESPONSE;
    }

}
