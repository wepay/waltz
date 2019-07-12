package com.wepay.waltz.storage.common.message.admin;

public class LastSessionInfoRequest extends AdminMessage {

    public final int partitionId;

    public LastSessionInfoRequest(long seqNum, int partitionId) {
        super(seqNum);

        this.partitionId = partitionId;
    }

    @Override
    public byte type() {
        return AdminMessageType.LAST_SESSION_INFO_REQUEST;
    }
}
