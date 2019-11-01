package com.wepay.waltz.common.message;

public class HighWaterMarkResponse extends AbstractMessage {

    public final long transactionId;

    public HighWaterMarkResponse(ReqId reqId, long transactionId) {
        super(reqId);
        this.transactionId = transactionId;
    }

    @Override
    public byte type() {
        return MessageType.HIGH_WATER_MARK_RESPONSE;
    }

}
