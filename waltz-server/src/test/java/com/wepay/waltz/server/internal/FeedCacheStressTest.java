package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore
public class FeedCacheStressTest {

    private static final int NUM_BLOCKS = 1000;
    private static final int FEED_CACHE_SIZE = FeedCache.BLOCK_SIZE * NUM_BLOCKS;
    private static final long DURATION = 100000;

    private final AtomicInteger putCount = new AtomicInteger();
    private final AtomicInteger getCount = new AtomicInteger();
    private final AtomicInteger eqCount = new AtomicInteger();
    private final AtomicInteger missCount = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean failed = new AtomicBoolean(false);

    private FeedCache cache;

    @Before
    public void setup() {
        putCount.set(0);
        getCount.set(0);
        eqCount.set(0);
        missCount.set(0);
        running.set(true);
        failed.set(false);

        cache = new FeedCache(FEED_CACHE_SIZE, null);
    }

    @After
    public void teardown() {
        cache.close();
    }

    @Test
    public void testSinglePartition() throws Exception {
        FeedCachePartition partition = cache.getPartition(0);

        PartitionTester partitionTester = new PartitionTester(partition);

        partitionTester.start();

        Uninterruptibly.sleep(DURATION);

        partitionTester.stop();
        partitionTester.join();

        assertEquals(cache.getPartitionSize(), partition.getNumBlocks());
        assertEquals(NUM_BLOCKS, cache.getSharedPoolSize() + partition.getNumBlocks());

        printResult();

        assertFalse(failed.get());
    }

    @Test
    public void testMultiplePartitions() throws Exception {
        int numPartitions = 10;
        List<PartitionTester> partitionTesters = new ArrayList<>();

        for (int i = 0; i < numPartitions; i++) {
            PartitionTester partitionTester = new PartitionTester(cache.getPartition(i));
            partitionTesters.add(partitionTester);
            partitionTester.start();
            Uninterruptibly.sleep(DURATION / 3 / (numPartitions - 1));
        }

        Uninterruptibly.sleep(DURATION / 3);

        for (int i = 0; i < numPartitions; i++) {
            PartitionTester partitionTester = partitionTesters.get(i);
            assertEquals(cache.getPartitionSize(), partitionTester.partition.getNumBlocks());
        }

        for (int i = 0; i < numPartitions; i++) {
            partitionTesters.get(i).stop();
            partitionTesters.get(i).join();
            partitionTesters.get(i).partition.close();
            Uninterruptibly.sleep(DURATION / 3 / (numPartitions - 1));
        }

        for (int i = 0; i < numPartitions; i++) {
            PartitionTester partitionTester = partitionTesters.get(i);
            assertEquals(0, partitionTester.partition.getNumBlocks());
        }
        assertEquals(NUM_BLOCKS, cache.getSharedPoolSize());

        printResult();

        assertFalse(failed.get());
    }

    private void printResult() {
        System.out.println(String.format("  puts: %12d",  putCount.get()));
        System.out.println(String.format("  gets: %12d",  getCount.get()));
        System.out.println(String.format("   eqs: %12d",  eqCount.get()));
        System.out.println(String.format("  miss: %12d",  missCount.get()));
        System.out.println(String.format("result: %12s",  failed.get() ? " FAIL" : "SUCCESS"));
    }

    private ReqId generateReqId(long txnId) {
        return new ReqId(txnId + 1000, txnId + 2000);
    }

    private int generateHeader(long txnId) {
        return (int) (txnId ^ 12345);
    }

    private class PartitionTester {

        public final FeedCachePartition partition;
        private final Thread writeThread;
        private final Thread readThread;
        private volatile boolean running = true;

        PartitionTester(FeedCachePartition partition) {
            this.partition = partition;

            // Create a list of feed data
            List<FeedData> feedDataList = new ArrayList<>();
            for (long txnId = 0; txnId < NUM_BLOCKS * FeedCacheBlock.NUM_TRANSACTIONS * 2; txnId++) {
                feedDataList.add(new FeedData(generateReqId(txnId), txnId, generateHeader(txnId)));
            }

            this.writeThread = new Thread(() -> {
                while (running && !failed.get()) {
                    for (FeedData feedData : feedDataList) {
                        partition.add(feedData.transactionId, feedData.reqId, feedData.header);
                        putCount.incrementAndGet();
                    }
                }
            });

            this.readThread = new Thread(() -> {
                Random rand = new Random();
                while (running && !failed.get()) {
                    int txnId = rand.nextInt(feedDataList.size());

                    FeedData data = partition.get(txnId);
                    getCount.incrementAndGet();

                    if (data != null) {
                        if (data.equals(feedDataList.get(txnId))) {
                            eqCount.incrementAndGet();
                        } else {
                            failed.set(true);
                        }
                    } else {
                        missCount.incrementAndGet();
                    }
                }
            });
        }

        void start() {
            writeThread.start();
            readThread.start();
        }

        void stop() {
            running = false;
        }

        void join() throws InterruptedException {
            writeThread.join();
            readThread.join();
        }

    }

}
