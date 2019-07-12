package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.message.FeedSuspended;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.message.FeedData;

import java.util.Comparator;

public class FeedContext {

    public static final Comparator<FeedContext> HIGH_WATER_MARK_COMPARATOR =
        (o1, o2) -> o1.highWaterMark < o2.highWaterMark ? -1 : o1.highWaterMark > o2.highWaterMark ? 1 : 0;

    public final ReqId reqId;
    public final PartitionClient sender;

    public final FeedSuspended suspendMessage;
    private long highWaterMark;
    private long remaining;

    public FeedContext(ReqId reqId, long highWaterMark, long fetchSize, PartitionClient sender, FeedSuspended suspendMessage) {
        this.reqId = reqId;
        this.highWaterMark = highWaterMark;
        this.remaining = fetchSize;
        this.sender = sender;
        this.suspendMessage = suspendMessage;
    }

    public long nextTransactionId() {
        return highWaterMark + 1;
    }

    public void send(FeedData data, boolean flush) {
        if (data.transactionId != highWaterMark + 1) {
            throw new IllegalStateException("out of order transactions");
        }
        if (remaining <= 0) {
            throw new IllegalStateException("no more fetch allowed");
        }

        highWaterMark = data.transactionId;
        remaining--;

        if (remaining > 0) {
            // Force flushing every 10 transactions
            sender.sendMessage(data, remaining % 10 == 0 || flush);
        } else {
            sender.sendMessage(data, false);

            // Send a suspend message. The client should send another feed request to continue
            sender.sendMessage(suspendMessage, true);
        }
    }

    public boolean isActive() {
        return sender.isActive();
    }

    public boolean hasMoreToFetch() {
        return remaining > 0;
    }

    public long highWaterMark() {
        return highWaterMark;
    }

    public boolean isWritable() {
        return sender.isWritable();
    }

    public String toString() {
        return "clientId=" + reqId.clientId() + " partitionId=" + reqId.partitionId() + " hwm=" + highWaterMark + " remaining=" + remaining;
    }

}
