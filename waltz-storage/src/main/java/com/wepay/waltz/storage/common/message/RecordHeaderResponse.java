package com.wepay.waltz.storage.common.message;

import com.wepay.waltz.common.message.RecordHeader;

public class RecordHeaderResponse extends StorageMessage {

    public final RecordHeader recordHeader;

    public RecordHeaderResponse(long sessionId, long seqNum, int partitionId, RecordHeader recordHeader) {
        super(sessionId, seqNum, partitionId);

        this.recordHeader = recordHeader;
    }

    @Override
    public byte type() {
        return StorageMessageType.RECORD_HEADER_RESPONSE;
    }

}
