package com.wepay.waltz.storage.common.message;

import com.wepay.waltz.common.message.RecordHeader;

import java.util.ArrayList;

public class RecordHeaderListResponse extends StorageMessage {

    public final ArrayList<RecordHeader> recordHeaders;

    public RecordHeaderListResponse(long sessionId, long seqNum, int partitionId, ArrayList<RecordHeader> recordHeaders) {
        super(sessionId, seqNum, partitionId);

        this.recordHeaders = recordHeaders;
    }

    @Override
    public byte type() {
        return StorageMessageType.RECORD_HEADER_LIST_RESPONSE;
    }

}
