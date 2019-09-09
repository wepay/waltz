package com.wepay.waltz.server.internal;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FeedCacheBlockKeyTest {

    @Test
    public void test() {
        Random rand = new Random();
        int partitionId = rand.nextInt(Integer.MAX_VALUE);
        long transactionId = (long) rand.nextInt(Integer.MAX_VALUE);

        FeedCacheBlockKey key1 = FeedCacheBlockKey.get(partitionId, transactionId);
        FeedCacheBlockKey key2 = FeedCacheBlockKey.get(otherPartitionId(partitionId), transactionId);

        assertNotEquals(key1, key2);

        long firstTransactionId;

        firstTransactionId = key1.firstTransactionId() - 64;
        for (int i = 0; i < 64; i++) {
            FeedCacheBlockKey key = FeedCacheBlockKey.get(partitionId, firstTransactionId + i);
            assertNotEquals(key1, key);
        }

        firstTransactionId = key1.firstTransactionId();
        for (int i = 0; i < 64; i++) {
            FeedCacheBlockKey key = FeedCacheBlockKey.get(partitionId, firstTransactionId + i);
            assertEquals(key1, key);
            assertEquals(key1.hashCode(), key.hashCode());
        }

        firstTransactionId = key1.firstTransactionId() + 64;
        for (int i = 0; i < 64; i++) {
            FeedCacheBlockKey key = FeedCacheBlockKey.get(partitionId, firstTransactionId + i);
            assertNotEquals(key1, key);
        }
    }

    private int otherPartitionId(int partitionId) {
        if (partitionId == 0) {
            return partitionId + 1;
        } else {
            return partitionId - 1;
        }
    }

}
