package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.ReqId;

/**
 * A block of a feed cache. It can hold up to 64 transactions.
 * This class not thread-safe.
 */
public class FeedCacheBlock {

    static final int SIZE = 8 + 8 + 64 * (16 + 4);
    static final long INDEX_MASK = 63;
    static final int NUM_TRANSACTIONS = (int) (INDEX_MASK + 1);

    private final long[] reqIdBits;
    private final int[] headers;

    private long firstTransactionId;
    private long usage;

    FeedCacheBlock() {
        this.reqIdBits = new long[2 * NUM_TRANSACTIONS];
        this.headers = new int[NUM_TRANSACTIONS];
        this.firstTransactionId = Long.MIN_VALUE;
        this.usage = 0L;
    }

    /**
     * Resets a given block.
     * @param key The key to a block that has to be reset.
     */
    public void reset(FeedCacheBlockKey key) {
        this.firstTransactionId = key.firstTransactionId();
        this.usage = 0L;
    }

    /**
     * Adds feed data (i.e. transaction information) to the feed cache block.
     * @param transactionId The transaction ID.
     * @param reqId The request ID.
     * @param header The header of the given transaction ID.
     * @return True if added successfully, otherwise returns False.
     */
    public boolean add(long transactionId, ReqId reqId, int header) {
        if (transactionId >= firstTransactionId && transactionId < firstTransactionId + NUM_TRANSACTIONS) {
            int index = (int) (transactionId & INDEX_MASK);
            int rindex = index << 1; // = 2 * i
            reqIdBits[rindex] = reqId.mostSigBits;
            reqIdBits[rindex + 1] = reqId.leastSigBits;
            headers[index] = header;
            usage |= (1L << index);

            return true;
        }

        return false;
    }

    /**
     * Returns feed data for a given transaction ID.
     * @param transactionId The transaction ID.
     * @return feed data for a given transaction ID.
     */
    public FeedData get(long transactionId) {
        if (transactionId >= firstTransactionId && transactionId < firstTransactionId + NUM_TRANSACTIONS) {
            int index = (int) (transactionId & INDEX_MASK);
            if (((usage >> index) & 1L) != 0) {
                int rindex = index << 1; // = 2 * i
                return new FeedData(new ReqId(reqIdBits[rindex], reqIdBits[rindex + 1]), transactionId, headers[index]);
            }
        }

        return null;
    }

    long getUsage() {
        return usage;
    }

}
