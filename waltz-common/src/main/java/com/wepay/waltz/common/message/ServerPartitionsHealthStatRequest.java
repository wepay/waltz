package com.wepay.waltz.common.message;

public class ServerPartitionsHealthStatRequest extends AbstractMessage {

    public ServerPartitionsHealthStatRequest(ReqId reqId) {
        super(reqId);
    }

    @Override
    public byte type() {
        return MessageType.SERVER_PARTITIONS_HEALTH_STAT_REQUEST;
    }
}
