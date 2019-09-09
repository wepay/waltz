package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FeedCachePartitionTest {

    private static final int NUM_BLOCKS = 50;
    private static final int FEED_CACHE_SIZE = FeedCache.BLOCK_SIZE * NUM_BLOCKS;

    private FeedCache cache;
    private int partitionId;
    private FeedCachePartition partition;

    @Before
    public void setup() {
        Random rand = new Random();
        cache = new FeedCache(FEED_CACHE_SIZE, null);
        partitionId = rand.nextInt(Integer.MAX_VALUE);
        partition = cache.getPartition(partitionId);
    }

    @After
    public void teardown() {
        cache.close();
    }

    @Test
    public void testAddAndGet() {
        // Add transactions (0 - 99)
        for (long txnId = 0; txnId < 100; txnId++) {
            partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        }

        for (long txnId = 0; txnId < 300; txnId++) {
            FeedData data = partition.get(txnId);
            if (txnId < 100) {
                assertNotNull("txnId=" + txnId, data);
                assertEquals(generateReqId(txnId), data.reqId);
                assertEquals(txnId, data.transactionId);
                assertEquals(generateHeader(txnId), data.header);
            } else {
                assertNull(data);
            }
        }

        // Add transactions (100 - 199)
        for (long txnId = 100; txnId < 200; txnId++) {
            partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        }

        for (long txnId = 0; txnId < 300; txnId++) {
            FeedData data = partition.get(txnId);

            if (txnId < 200) {
                assertNotNull("txnId=" + txnId, data);
                assertEquals(generateReqId(txnId), data.reqId);
                assertEquals(txnId, data.transactionId);
                assertEquals(generateHeader(txnId), data.header);
            } else {
                assertNull(data);
            }
        }

        // Add transactions (200 - 299)
        for (long txnId = 200; txnId < 300; txnId++) {
            partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        }

        for (int txnId = 0; txnId < 300; txnId++) {
            FeedData data = partition.get(txnId);

            assertEquals(generateReqId(txnId), data.reqId);
            assertEquals(txnId, data.transactionId);
            assertEquals(generateHeader(txnId), data.header);
        }
    }

    @Test
    public void testLargeTransactionIds() {
        long largeTxnId = (long) Integer.MAX_VALUE + 50L; // txnIds are around Integer.MAX_VALUE

        // Add transactions
        for (long txnId = largeTxnId - 100; txnId < largeTxnId; txnId++) {
            partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        }

        for (long txnId = largeTxnId - 100; txnId < largeTxnId; txnId++) {
            FeedData data = partition.get(txnId);

            assertEquals(generateReqId(txnId), data.reqId);
            assertEquals(txnId, data.transactionId);
            assertEquals(generateHeader(txnId), data.header);
        }
    }

    @Test
    public void testAddAll() {
        // Create a list of RecordHeaders
        List<RecordHeader> recordHeaderList = new ArrayList<>();
        for (long txnId = 0; txnId < NUM_BLOCKS * FeedCacheBlock.NUM_TRANSACTIONS; txnId++) {
            recordHeaderList.add(new RecordHeader(txnId, generateReqId(txnId), generateHeader(txnId)));
        }

        // Add transactions
        partition.addAll(recordHeaderList);

        for (RecordHeader recordHeader : recordHeaderList) {
            FeedData data = partition.get(recordHeader.transactionId);
            assertNotNull("txnId=" + recordHeader.transactionId, data);
            assertEquals(recordHeader.transactionId, data.transactionId);
            assertEquals(generateReqId(recordHeader.transactionId), data.reqId);
            assertEquals(generateHeader(recordHeader.transactionId), data.header);
        }

        assertNull(partition.get(NUM_BLOCKS * FeedCacheBlock.NUM_TRANSACTIONS));

        partition.clear();

        // Create a new list of RecordHeaders
        recordHeaderList.clear();
        for (long i = 0; i <  NUM_BLOCKS * FeedCacheBlock.NUM_TRANSACTIONS; i++) {
            long txnId = NUM_BLOCKS * FeedCacheBlock.NUM_TRANSACTIONS + i;
            recordHeaderList.add(new RecordHeader(txnId, generateReqId(txnId), generateHeader(txnId)));
        }

        // Add transactions in a random order
        Collections.shuffle(recordHeaderList);
        partition.addAll(recordHeaderList);

        for (RecordHeader recordHeader : recordHeaderList) {
            FeedData data = partition.get(recordHeader.transactionId);
            assertNotNull("txnId=" + recordHeader.transactionId, data);
            assertEquals(recordHeader.transactionId, data.transactionId);
            assertEquals(generateReqId(recordHeader.transactionId), data.reqId);
            assertEquals(generateHeader(recordHeader.transactionId), data.header);
        }

        assertNull(partition.get(0));
    }

    @Test
    public void testBlocksSurvivingInGlobalCache() {
        // Restrict the max number of blocks per partition to 10.
        int maxNumBlocks = 10;
        partition.setMaxNumBlocks(maxNumBlocks);

        // Add many transactions. Make sure some blocks are evicted from the cache partition.
        for (long txnId = 0; txnId < 2000; txnId++) {
            partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
            assertTrue(partition.getNumBlocks() <= maxNumBlocks);
        }

        // Get transactions. Evicted blocks are still valid in the global cache. All get() calls should return data.
        for (long txnId = 0; txnId < 2000; txnId++) {
            FeedData data = partition.get(txnId);
            assertNotNull("txnId=" + txnId, data);
            assertEquals(generateReqId(txnId), data.reqId);
            assertEquals(txnId, data.transactionId);
            assertEquals(generateHeader(txnId), data.header);
        }
    }

    @Test
    public void testResizing() {
        // Set the max number of blocks per partition to 20.
        partition.setMaxNumBlocks(20);

        // Add many transactions.
        for (long txnId = 0; txnId < 2000; txnId++) {
            partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        }
        assertEquals(20, partition.getNumBlocks());

        partition.setMaxNumBlocks(10);
        assertEquals(10, partition.getNumBlocks());

        partition.setMaxNumBlocks(15);
        assertEquals(10, partition.getNumBlocks());

        // Add more transactions.
        for (long txnId = 2000; txnId < 4000; txnId++) {
            partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        }
        assertEquals(15, partition.getNumBlocks());

        partition.setMaxNumBlocks(20);
        assertEquals(15, partition.getNumBlocks());
    }

    @Test
    public void testClose() {
        int numBlocks = cache.getSharedPoolSize();

        assertEquals(1, cache.getNumPartitions());

        long txnId = 1000;
        partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        assertEquals(numBlocks - 1, cache.getSharedPoolSize());

        partition.close();

        assertEquals(0, cache.getNumPartitions());
        assertEquals(numBlocks, cache.getSharedPoolSize());

        // Get will return null
        assertNull(partition.get(txnId));

        // Add will be ignored
        partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        assertEquals(0, cache.getNumPartitions());
        assertEquals(numBlocks, cache.getSharedPoolSize());
    }

    @Test
    public void testRefCount() {
        FeedCachePartition duplicate = cache.getPartition(partitionId);
        assertSame(partition, duplicate);

        int numBlocks = cache.getSharedPoolSize();

        assertEquals(1, cache.getNumPartitions());

        long txnId = 1000;
        partition.add(txnId, generateReqId(txnId), generateHeader(txnId));
        assertEquals(numBlocks - 1, cache.getSharedPoolSize());

        partition.close();

        // The partition should be still open because of the reference count.
        FeedData data = partition.get(txnId);
        assertNotNull("txnId=" + txnId, data);
        assertEquals(generateReqId(txnId), data.reqId);
        assertEquals(txnId, data.transactionId);
        assertEquals(generateHeader(txnId), data.header);

        assertEquals(1, cache.getNumPartitions());
        assertEquals(numBlocks - 1, cache.getSharedPoolSize());

        // This will actually close the partition.
        duplicate.close();

        assertEquals(0, cache.getNumPartitions());
        assertEquals(numBlocks, cache.getSharedPoolSize());

        // Get will return null
        assertNull(partition.get(txnId));
    }

    private ReqId generateReqId(long txnId) {
        return new ReqId(txnId + 1000, txnId + 2000);
    }

    private int generateHeader(long txnId) {
        return (int) (txnId ^ 12345);
    }

}
