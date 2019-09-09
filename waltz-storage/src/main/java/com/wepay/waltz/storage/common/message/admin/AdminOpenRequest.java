package com.wepay.waltz.storage.common.message.admin;

import java.util.UUID;

public class AdminOpenRequest extends AdminMessage {

    public final UUID key;
    public final int numPartitions;

    public AdminOpenRequest(UUID key, int numPartitions) {
        super(-1L);

        this.key = key;
        this.numPartitions = numPartitions;
    }

    @Override
    public byte type() {
        return AdminMessageType.OPEN_REQUEST;
    }

}
