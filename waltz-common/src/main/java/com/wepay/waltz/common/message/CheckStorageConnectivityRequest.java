package com.wepay.waltz.common.message;

public class CheckStorageConnectivityRequest extends AbstractMessage {

    public CheckStorageConnectivityRequest(ReqId reqId) {
        super(reqId);
    }

    @Override
    public byte type() {
        return MessageType.CHECK_STORAGE_CONNECTIVITY_REQUEST;
    }
}
