package com.wepay.waltz.common.message;

public class FeedRequest extends AbstractMessage {

    public final long clientHighWaterMark;

    public FeedRequest(ReqId reqId, long clientHighWaterMark) {
        super(reqId);
        this.clientHighWaterMark = clientHighWaterMark;
    }

    @Override
    public byte type() {
        return MessageType.FEED_REQUEST;
    }

}
