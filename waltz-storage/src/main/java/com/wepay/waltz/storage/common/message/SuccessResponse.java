package com.wepay.waltz.storage.common.message;

public class SuccessResponse extends StorageMessage {

    public SuccessResponse(long sessionId, long seqNum, int partitionId) {
        super(sessionId, seqNum, partitionId);
    }

    @Override
    public byte type() {
        return StorageMessageType.SUCCESS_RESPONSE;
    }

}
