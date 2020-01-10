package com.wepay.waltz.common.message;

import java.util.Map;

public class CheckStorageConnectivityResponse extends AbstractMessage {

    public final Map<String, Boolean> storageConnectivityMap;
    public CheckStorageConnectivityResponse(ReqId reqId, Map<String, Boolean> storageConnectivityMap) {
        super(reqId);
        this.storageConnectivityMap = storageConnectivityMap;
    }

    @Override
    public byte type() {
        return MessageType.CHECK_STORAGE_CONNECTIVITY_RESPONSE;
    }
}
