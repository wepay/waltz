package com.wepay.waltz.common.message;

public class ServerPartitionsInfoRequest extends AbstractMessage {

    public ServerPartitionsInfoRequest(ReqId reqId) {
        super(reqId);
    }

    @Override
    public byte type() {
        return MessageType.SERVER_PARTITIONS_INFO_REQUEST;
    }
}
