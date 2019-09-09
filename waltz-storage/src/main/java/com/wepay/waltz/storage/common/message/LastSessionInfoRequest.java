package com.wepay.waltz.storage.common.message;

public class LastSessionInfoRequest extends StorageMessage {

    public LastSessionInfoRequest(long sessionId, long seqNum, int partitionId) {
        this(sessionId, seqNum, partitionId, false);
    }

    public LastSessionInfoRequest(long sessionId, long seqNum, int partitionId, boolean usedByOfflineRecovery) {
        super(sessionId, seqNum, partitionId, usedByOfflineRecovery);
    }

    @Override
    public byte type() {
        return StorageMessageType.LAST_SESSION_INFO_REQUEST;
    }
}
