package com.wepay.waltz.common.message;

public class HighWaterMarkRequest extends AbstractMessage {

    public HighWaterMarkRequest(ReqId reqId) {
        super(reqId);
    }

    @Override
    public byte type() {
        return MessageType.HIGH_WATER_MARK_REQUEST;
    }

}
