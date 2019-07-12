package com.wepay.waltz.common.message;

public class FlushResponse extends AbstractMessage {

    public final long transactionId;

    public FlushResponse(ReqId reqId, long transactionId) {
        super(reqId);
        this.transactionId = transactionId;
    }

    @Override
    public byte type() {
        return MessageType.FLUSH_RESPONSE;
    }

}
