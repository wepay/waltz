package com.wepay.waltz.common.message;

public class FlushRequest extends AbstractMessage {

    public FlushRequest(ReqId reqId) {
        super(reqId);
    }

    @Override
    public byte type() {
        return MessageType.FLUSH_REQUEST;
    }

}
