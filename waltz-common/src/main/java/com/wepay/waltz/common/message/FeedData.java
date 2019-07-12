package com.wepay.waltz.common.message;

public class FeedData extends AbstractMessage {

    public final long transactionId;
    public final int header;

    public FeedData(ReqId reqId, long transactionId, int header) {
        super(reqId);
        this.transactionId = transactionId;
        this.header = header;
    }

    @Override
    public byte type() {
        return MessageType.FEED_DATA;
    }

    @Override
    public int hashCode() {
        return reqId.hashCode() ^ (int) (transactionId % 0xFFFFFFFF) ^ header;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof FeedData) {
            FeedData other = (FeedData) o;
            return this.reqId.equals(other.reqId)
                && this.transactionId == other.transactionId
                && this.header == other.header;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "FeedData(reqId=" + reqId.toString() + ", transactionId=" + transactionId + " header=" + Integer.toHexString(header) + ")";
    }

}
