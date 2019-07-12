package com.wepay.waltz.storage.common.message;

import com.wepay.waltz.common.message.Record;

public class RecordResponse extends StorageMessage {

    public final Record record;

    public RecordResponse(long sessionId, long seqNum, int partitionId, Record record) {
        super(sessionId, seqNum, partitionId);

        this.record = record;
    }

    @Override
    public byte type() {
        return StorageMessageType.RECORD_RESPONSE;
    }

}
