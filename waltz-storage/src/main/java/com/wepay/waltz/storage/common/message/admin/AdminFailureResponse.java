package com.wepay.waltz.storage.common.message.admin;

import com.wepay.waltz.storage.exception.StorageRpcException;

public class AdminFailureResponse extends AdminMessage {

    public final StorageRpcException exception;

    public AdminFailureResponse(long seqNum, StorageRpcException exception) {
        super(seqNum);

        this.exception = exception;
    }

    @Override
    public byte type() {
        return AdminMessageType.FAILURE_RESPONSE;
    }


}
