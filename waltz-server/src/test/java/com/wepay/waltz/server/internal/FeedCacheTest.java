package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.store.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FeedCacheTest {

    private static final int NUM_BLOCKS = 1000;
    private static final int FEED_CACHE_SIZE = FeedCache.BLOCK_SIZE * NUM_BLOCKS;

    private FeedCache cache;

    @Before
    public void setup() {
        cache = new FeedCache(FEED_CACHE_SIZE, null);
    }

    @After
    public void teardown() {
        cache.close();
    }

    @Test
    public void testMultiplePartitions() {
        int totalNumBlocks = cache.getSharedPoolSize();
        FeedCachePartition partition0 = cache.getPartition(0);
        FeedCachePartition partition1 = cache.getPartition(1);

        FeedData[] data0 = fillPartition(partition0);

        for (FeedData feedData : data0) {
            partition0.get(feedData.transactionId);
            assertEquals(feedData, partition0.get(feedData.transactionId));
        }

        FeedData[] data1 = fillPartition(partition1);

        for (FeedData feedData : data1) {
            partition1.get(feedData.transactionId);
            assertEquals(feedData, partition1.get(feedData.transactionId));
        }

        // The partition 0 should not be affected.
        for (FeedData feedData : data0) {
            partition0.get(feedData.transactionId);
            assertEquals(feedData, partition0.get(feedData.transactionId));
        }

        // Flood the partition 1.
        for (int i = data1.length; i < totalNumBlocks * FeedCacheBlock.NUM_TRANSACTIONS; i++) {
            partition1.add(i, TestUtils.reqId(), 0);
        }

        // The partition 0 should not be affected.
        for (FeedData feedData : data0) {
            partition0.get(feedData.transactionId);
            assertEquals(feedData, partition0.get(feedData.transactionId));
        }
    }

    @Test
    public void testPartitionResizing() {
        int totalNumBlocks = cache.getSharedPoolSize();
        int numPartitions = 0;
        int expectedMaxNumBlocks = 0;

        FeedCachePartition partition0 = cache.getPartition(0);
        numPartitions++;
        expectedMaxNumBlocks = totalNumBlocks / (numPartitions + 1);
        assertEquals(expectedMaxNumBlocks, partition0.getMaxNumBlocks());

        FeedCachePartition partition1 = cache.getPartition(1);
        numPartitions++;
        expectedMaxNumBlocks = totalNumBlocks / (numPartitions + 1);
        assertEquals(expectedMaxNumBlocks, partition0.getMaxNumBlocks());
        assertEquals(expectedMaxNumBlocks, partition1.getMaxNumBlocks());

        FeedCachePartition partition2 = cache.getPartition(2);
        numPartitions++;
        expectedMaxNumBlocks = totalNumBlocks / (numPartitions + 1);
        assertEquals(expectedMaxNumBlocks, partition0.getMaxNumBlocks());
        assertEquals(expectedMaxNumBlocks, partition1.getMaxNumBlocks());
        assertEquals(expectedMaxNumBlocks, partition2.getMaxNumBlocks());

        partition0.close();
        numPartitions--;
        expectedMaxNumBlocks = totalNumBlocks / (numPartitions + 1);
        assertEquals(expectedMaxNumBlocks, partition1.getMaxNumBlocks());
        assertEquals(expectedMaxNumBlocks, partition2.getMaxNumBlocks());

        partition1.close();
        numPartitions--;
        expectedMaxNumBlocks = totalNumBlocks / (numPartitions + 1);
        assertEquals(expectedMaxNumBlocks, partition2.getMaxNumBlocks());
    }

    private FeedData[] fillPartition(FeedCachePartition partition) {
        FeedData[] data = new FeedData[partition.getMaxNumBlocks() * FeedCacheBlock.NUM_TRANSACTIONS];

        for (int i = 0; i < data.length; i++) {
            data[i] = new FeedData(TestUtils.reqId(), i, 0);
            partition.add(data[i].transactionId, data[i].reqId, data[i].header);
        }

        return data;
    }

}
