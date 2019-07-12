package com.wepay.waltz.common.message;

public class FeedSuspended extends AbstractMessage {

    public FeedSuspended(ReqId reqId) {
        super(reqId);
    }

    @Override
    public byte type() {
        return MessageType.FEED_SUSPENDED;
    }

}
