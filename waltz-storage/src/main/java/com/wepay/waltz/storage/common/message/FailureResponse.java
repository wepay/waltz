package com.wepay.waltz.storage.common.message;

import com.wepay.waltz.storage.exception.StorageRpcException;

public class FailureResponse extends StorageMessage {

    public final StorageRpcException exception;

    public FailureResponse(long sessionId, long seqNum, int partitionId, StorageRpcException exception) {
        super(sessionId, seqNum, partitionId);

        this.exception = exception;
    }

    @Override
    public byte type() {
        return StorageMessageType.FAILURE_RESPONSE;
    }

}
