package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.util.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionCacheTest {

    private static final int CACHE_SIZE = 6000;
    private static final int MAX_SERIALIZED_SIZE = 500;
    private static final int MAX_DATA_SIZE = MAX_SERIALIZED_SIZE - TransactionKey.SERIALIZED_KEY_SIZE - TransactionData.RECORD_OVERHEAD;

    private TransactionCache cache = null;

    @Before
    public void setup() {
        cache = new TransactionCache(CACHE_SIZE, MAX_SERIALIZED_SIZE, false, null);
    }

    @Test
    public void testCachingSmallSizedItems() {
        testCaching(cache.size() / 1000, MAX_DATA_SIZE / 2);
    }

    @Test
    public void testCachingLargeSizedItems() {
        testCaching(MAX_DATA_SIZE, MAX_DATA_SIZE);
    }

    @Test
    public void testCachingAllSizedItems() {
        testCaching(cache.size() / 1000, MAX_DATA_SIZE);
    }

    private void testCaching(int minDataSize, int maxDataSize) {
        assertTrue(maxDataSize <= MAX_DATA_SIZE);

        TransactionKey[] txnKeys = new TransactionKey[1000];
        TransactionData[] txnData = new TransactionData[1000];
        int evictedSize = 0;
        Random rand = new Random();

        for (int write = 0; write < txnData.length; write++) {
            int partitionId = rand.nextInt(10);
            int transactionId = rand.nextInt(Integer.MAX_VALUE);
            int dataSize;

            if (minDataSize == maxDataSize) {
                dataSize = maxDataSize;
            } else {
                dataSize = minDataSize + rand.nextInt(maxDataSize - minDataSize);
            }

            byte[] data = new byte[dataSize];
            rand.nextBytes(data);
            int checksum = Utils.checksum(data);

            txnKeys[write] = new TransactionKey(partitionId, transactionId);
            txnData[write] = new TransactionData(data, checksum);

            cache.put(txnKeys[write], txnData[write]);

            int retainedSize = 0;
            int read = 0;

            // Skip evicted items
            while (read <= write) {
                TransactionData cached = cache.get(new TransactionKey(txnKeys[read].partitionId, txnKeys[read].transactionId));

                if (cached == null) {
                    evictedSize += (TransactionKey.SERIALIZED_KEY_SIZE + txnData[read].serializedSize());
                    read++;

                } else {
                    break;
                }
            }

            while (read <= write) {
                TransactionData cached = cache.get(new TransactionKey(txnKeys[read].partitionId, txnKeys[read].transactionId));

                if (cached != null) {
                    if (!txnData[read].equals(cached)) {
                        System.out.println();
                        cache.get(new TransactionKey(txnKeys[read].partitionId, txnKeys[read].transactionId));
                    }
                    assertEquals(txnData[read], cached);
                    retainedSize += (TransactionKey.SERIALIZED_KEY_SIZE + txnData[read].serializedSize());
                    read++;

                } else {
                    // Newer items must be found.
                    fail();
                }
            }

            if (evictedSize > 0) {
                // Cache is near full.
                assertTrue(retainedSize + (TransactionKey.SERIALIZED_KEY_SIZE + TransactionData.RECORD_OVERHEAD + maxDataSize) * 2 >= cache.size());
            } else {
                // All items are retained.
                assertTrue(retainedSize <= cache.size());
            }
        }
    }

    @Test
    public void testExcessivelyLargeItemsNotCached() {
        Random rand = new Random();

        int start = MAX_DATA_SIZE - 10;
        int end = MAX_DATA_SIZE + 10;

        for (int dataSize = start; dataSize < end; dataSize++) {
            int partitionId = rand.nextInt(10);
            int transactionId = rand.nextInt(Integer.MAX_VALUE);
            byte[] data = new byte[dataSize];
            rand.nextBytes(data);
            int checksum = Utils.checksum(data);

            TransactionKey txnKey = new TransactionKey(partitionId, transactionId);
            TransactionData txnData = new TransactionData(data, checksum);
            cache.put(txnKey, txnData);

            TransactionData cached = cache.get(new TransactionKey(partitionId, transactionId));
            if (dataSize <= MAX_DATA_SIZE) {
                assertNotNull(cached);
                assertEquals(txnData, cached);
            } else {
                assertNull(cached);
            }
        }
    }

}
