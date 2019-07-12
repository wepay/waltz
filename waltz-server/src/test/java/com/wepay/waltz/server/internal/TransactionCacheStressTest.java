package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.util.Utils;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;
import org.junit.Ignore;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;

@Ignore
public class TransactionCacheStressTest {

    private static final int MAX_SERIALIZED_SIZE = 2000;
    private static final int CACHE_SIZE = 6000;

    @Test
    public void testHeapBuffer() throws Exception {
        test(false, 100000);
    }

    @Test
    public void testDirectBuffer() throws Exception {
        test(true, 100000);
    }

    private void test(boolean direct, long duration) throws Exception {
        TransactionCache cache = new TransactionCache(CACHE_SIZE, MAX_SERIALIZED_SIZE, direct, null);
        int maxTransactionDataSize = MAX_SERIALIZED_SIZE - TransactionData.RECORD_OVERHEAD;
        int minTransactionDataSize = maxTransactionDataSize * 7 / 10;

        int numTxns = CACHE_SIZE / minTransactionDataSize + 1;
        TransactionKey[] txnKeys = new TransactionKey[numTxns];
        TransactionData[] txnData = new TransactionData[numTxns];

        Random rand = new Random();

        for (int i = 0; i < numTxns; i++) {
            int partitionId = rand.nextInt(10);
            int transactionId = rand.nextInt(Integer.MAX_VALUE);
            int dataSize = minTransactionDataSize + rand.nextInt(maxTransactionDataSize - minTransactionDataSize);

            byte[] data = new byte[dataSize];
            rand.nextBytes(data);
            int checksum = Utils.checksum(data);

            txnKeys[i] = new TransactionKey(partitionId, transactionId);
            txnData[i] = new TransactionData(data, checksum);
        }

        AtomicInteger putCount = new AtomicInteger();
        AtomicInteger getCount = new AtomicInteger();
        AtomicInteger eqCount = new AtomicInteger();
        AtomicInteger missCount = new AtomicInteger();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean failed = new AtomicBoolean(false);

        Thread writeThread = new Thread(() -> {
            int n = 0;
            while (running.get()) {
                cache.put(txnKeys[n], txnData[n]);
                putCount.incrementAndGet();
                n = (n + 1) % numTxns;
            }
        });

        Thread[] readThreads = new Thread[10];
        for (int i = 0; i < readThreads.length; i++) {
            readThreads[i] = new Thread(() -> {
                while (running.get()) {
                    int n = rand.nextInt(numTxns);
                    TransactionData data = cache.get(txnKeys[n]);
                    getCount.incrementAndGet();

                    if (data != null) {
                        if (data.equals(txnData[n])) {
                            eqCount.incrementAndGet();
                        } else {
                            running.set(false);
                            failed.set(true);
                        }
                    } else {
                        missCount.incrementAndGet();
                    }
                }
            });
        }

        writeThread.start();
        for (Thread readThread : readThreads) {
            readThread.start();
        }

        Uninterruptibly.sleep(duration);
        running.set(false);

        for (Thread readThread : readThreads) {
            readThread.join();
        }
        writeThread.join();

        System.out.println(String.format("  puts: %12d",  putCount.get()));
        System.out.println(String.format("  gets: %12d",  getCount.get()));
        System.out.println(String.format("   eqs: %12d",  eqCount.get()));
        System.out.println(String.format("  miss: %12d",  missCount.get()));
        System.out.println(String.format("result: %12s",  failed.get() ? " FAIL" : "SUCCESS"));

        assertFalse(failed.get());
    }

}
