package com.wepay.waltz.storage.common.message;

import com.wepay.waltz.common.message.Record;

import java.util.ArrayList;

public class AppendRequest extends StorageMessage {

    public final ArrayList<Record> records;

    public AppendRequest(long sessionId, long seqNum, int partitionId, ArrayList<Record> records) {
        this(sessionId, seqNum, partitionId, records, false);
    }

    public AppendRequest(long sessionId, long seqNum, int partitionId, ArrayList<Record> records, boolean usedByOfflineRecovery) {
        super(sessionId, seqNum, partitionId, usedByOfflineRecovery);

        this.records = records;
    }

    @Override
    public byte type() {
        return StorageMessageType.APPEND_REQUEST;
    }
}
