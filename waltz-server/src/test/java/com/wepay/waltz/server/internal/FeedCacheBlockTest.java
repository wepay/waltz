package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.store.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FeedCacheBlockTest {

    @Test
    public void testReset() {
        Random rand = new Random();
        int partitionId = rand.nextInt(Integer.MAX_VALUE);
        long transactionId = (long) rand.nextInt(Integer.MAX_VALUE);
        int header = rand.nextInt();
        ReqId reqId = TestUtils.reqId();

        FeedCacheBlock block = new FeedCacheBlock();

        assertFalse(block.add(transactionId, reqId, header));

        FeedCacheBlockKey key = FeedCacheBlockKey.get(partitionId, transactionId);

        block.reset(key);

        assertTrue(block.add(transactionId, reqId, header));

        FeedData feedData = block.get(transactionId);
        FeedData expectedFeedData = new FeedData(reqId, transactionId, header);

        assertEquals(expectedFeedData, feedData);

        block.reset(key);
        assertNull(block.get(transactionId));
    }

    @Test
    public void testAddAndGet() {
        Random rand = new Random();
        int partitionId = rand.nextInt(Integer.MAX_VALUE);
        long startTransactionId = ((long) rand.nextInt(Integer.MAX_VALUE)) << 6;
        long expectedUsage;

        List<FeedData> feeds = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            feeds.add(new FeedData(TestUtils.reqId(), startTransactionId + i, rand.nextInt()));
        }

        FeedCacheBlock block = new FeedCacheBlock();
        FeedCacheBlockKey key = FeedCacheBlockKey.get(partitionId, startTransactionId);

        // Ordered
        block.reset(key);
        expectedUsage = 0L;

        for (int i = 0; i < 64; i++) {
            assertTrue(block.add(feeds.get(i).transactionId, feeds.get(i).reqId, feeds.get(i).header));
            expectedUsage |= (1L << i);
            assertEquals(expectedUsage, block.getUsage());
        }

        for (int i = 0; i < 64; i++) {
            assertEquals(feeds.get(i), block.get(feeds.get(i).transactionId));
        }

        // Random
        Collections.shuffle(feeds);
        block.reset(key);
        expectedUsage = 0L;

        for (int i = 0; i < 64; i++) {
            assertTrue(block.add(feeds.get(i).transactionId, feeds.get(i).reqId, feeds.get(i).header));
            expectedUsage |= (1L << (feeds.get(i).transactionId & FeedCacheBlock.INDEX_MASK));
            assertEquals(expectedUsage, block.getUsage());
        }

        for (int i = 0; i < 64; i++) {
            assertEquals(feeds.get(i), block.get(feeds.get(i).transactionId));
        }

        // Reset
        block.reset(key);
        for (int i = 0; i < 64; i++) {
            assertNull(block.get(feeds.get(i).transactionId));
        }
    }

}
