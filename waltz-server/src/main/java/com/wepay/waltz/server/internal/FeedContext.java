package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.message.FeedSuspended;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.message.FeedData;

import java.util.Comparator;

/**
 * This class takes care of bringing the client up to date with the current high-water mark.
 */
public class FeedContext {

    public static final Comparator<FeedContext> HIGH_WATER_MARK_COMPARATOR =
        (o1, o2) -> {
            int hwmCompare = Long.compare(o1.highWaterMark, o2.highWaterMark);

            if (hwmCompare == 0) {
                return Integer.compare(o1.reqId.seqNum(), o2.reqId.seqNum());
            }

            return hwmCompare;
        };

    public final ReqId reqId;
    public final PartitionClient sender;

    public final FeedSuspended suspendMessage;
    private long highWaterMark;
    private long remaining;

    /**
     * Class constructor.
     * @param reqId The request ID.
     * @param highWaterMark The client high-water mark received in the request.
     * @param fetchSize This size represents how far the client high-water mark is behind.
     * @param sender The client that has sent the request.
     * @param suspendMessage The {@link com.wepay.waltz.common.message.MountResponse} to be sent back to the client.
     */
    public FeedContext(ReqId reqId, long highWaterMark, long fetchSize, PartitionClient sender, FeedSuspended suspendMessage) {
        this.reqId = reqId;
        this.highWaterMark = highWaterMark;
        this.remaining = fetchSize;
        this.sender = sender;
        this.suspendMessage = suspendMessage;
    }

    /**
     * Returns the next expected transaction ID.
     * @return the next expected transaction ID.
     */
    public long nextTransactionId() {
        return highWaterMark + 1;
    }

    /**
     * Send data to the client for it to catch up on its high-water mark.
     * @param data The {@link FeedData} to be sent to the client.
     * @param flush If true then write and flush the message, otherwise only write the message.
     */
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

    /**
     * Returns True if the client is active, otherwise returns False.
     * @return True if the client is active, otherwise returns False.
     */
    public boolean isActive() {
        return sender.isActive();
    }

    /**
     * Returns True if the client high-water is not up to date, otherwise returns False.
     * @return True if the client high-water is not up to date, otherwise returns False.
     */
    public boolean hasMoreToFetch() {
        return remaining > 0;
    }

    /**
     * Returns current high-water mark of the client.
     * @return current high-water mark of the client.
     */
    public long highWaterMark() {
        return highWaterMark;
    }

    /**
     * Returns True if the client is writable, otherwise returns False.
     * @return True if the client is writable, otherwise returns False.
     */
    public boolean isWritable() {
        return sender.isWritable();
    }

    /**
     * Returns a string message that contains the information about the client.
     * @return a string message that contains the information about the client.
     */
    public String toString() {
        return "clientId=" + reqId.clientId() + " partitionId=" + reqId.partitionId() + " hwm=" + highWaterMark + " remaining=" + remaining;
    }

}
