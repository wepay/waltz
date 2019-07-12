package com.wepay.waltz.storage.common.message;

import java.util.UUID;

public class OpenRequest extends StorageMessage {

    public final UUID key;
    public final int numPartitions;

    public OpenRequest(UUID key, int numPartitions) {
        super(-1L, -1L, -1);

        this.key = key;
        this.numPartitions = numPartitions;
    }

    @Override
    public byte type() {
        return StorageMessageType.OPEN_REQUEST;
    }

}
