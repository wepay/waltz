package com.wepay.waltz.storage.common.message;

public class SetLowWaterMarkRequest extends StorageMessage {

    public final long lowWaterMark;

    public SetLowWaterMarkRequest(long sessionId, long seqNum, int partitionId, long lowWaterMark) {
        this(sessionId, seqNum, partitionId, lowWaterMark, false);
    }

    public SetLowWaterMarkRequest(long sessionId, long seqNum, int partitionId, long lowWaterMark, boolean usedByOfflineRecovery) {
        super(sessionId, seqNum, partitionId, usedByOfflineRecovery);

        this.lowWaterMark = lowWaterMark;
    }

    @Override
    public byte type() {
        return StorageMessageType.SET_LOW_WATER_MARK_REQUEST;
    }
}
