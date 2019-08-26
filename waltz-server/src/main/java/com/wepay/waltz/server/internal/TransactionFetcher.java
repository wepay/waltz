package com.wepay.waltz.server.internal;

import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.util.Logging;
import com.wepay.riff.util.RequestQueue;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.util.QueueConsumerTask;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.store.exception.TransactionNotFoundException;
import org.slf4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Used to fetch the transaction data from the {@link StorePartition}. Unique for each Waltz-Server.
 */
public class TransactionFetcher {

    private static final Logger logger = Logging.getLogger(TransactionFetcher.class);
    private static final int QUEUE_SIZE = 100;

    private final TransactionCache cache;
    private final ConcurrentHashMap<TransactionKey, CompletableFuture<TransactionData>> futures;
    private final CachingTask task;

    /**
     * Class constructor.
     * @param cacheSize The maximum size of the transaction cache.
     * @param directAllocation If True, allocates a new direct byte buffer (outside Head memory) else a new byte buffer (from Heap memory).
     * @param cacheMissMeter Metric to track cache miss.
     */
    public TransactionFetcher(int cacheSize, boolean directAllocation, Meter cacheMissMeter) {
        this.cache = new TransactionCache(cacheSize, cacheSize / 4, directAllocation, cacheMissMeter);
        this.futures = new ConcurrentHashMap<>();
        this.task = new CachingTask(QUEUE_SIZE);
        this.task.start();
    }

    /**
     * Stops the task that is responsible for enqueuing the transaction to the cache asynchronously.
     */
    public void close() {
        task.stop();
    }

    /**
     * Adds the transaction information to the cache.
     * @param key Transaction data key.
     * @param data The data to be written to a partition.
     */
    public void cache(TransactionKey key, TransactionData data) {
        CompletableFuture<TransactionData> future = CompletableFuture.completedFuture(data);

        if (futures.putIfAbsent(key, future) == null) {
            // Enqueue the transaction to cache it asynchronously
            if (!task.enqueue(new Item(key, data))) {
                futures.remove(key);
            }
        }
    }

    /**
     * Returns the transaction data for a specific {@link TransactionKey} from the given {@link StorePartition}.
     * @param key Transaction data key.
     * @param storePartition {@link StorePartition} associated with the given partition ID.
     * @return Transaction data for the given key.
     * @throws StoreException thrown if {@code StorePartition} is closed.
     */
    public TransactionData fetch(TransactionKey key, StorePartition storePartition) throws StoreException {
        CompletableFuture<TransactionData> existingFuture = futures.get(key);

        if (existingFuture == null) {
            CompletableFuture<TransactionData> future = new CompletableFuture<>();
            existingFuture = futures.putIfAbsent(key, future);

            if (existingFuture == null) {
                // This thread owns the future.
                try {
                    // Try to fetch from the cache
                    TransactionData data = cache.get(key);

                    if (data == null) {
                        // Try to fetch from the store.
                        Record record = storePartition.getRecord(key.transactionId);
                        if (record == null) {
                            throw new TransactionNotFoundException(key.partitionId, key.transactionId);
                        }

                        data = new TransactionData(record.data, record.checksum);
                    }

                    future.complete(data);

                    // Enqueue the transaction data to cache asynchronously. The future will be removed when the caching completes.
                    if (!task.enqueue(new Item(key, data))) {
                        futures.remove(key);
                    }

                    return data;

                } catch (Throwable ex) {
                    future.completeExceptionally(ex);
                    throw ex;
                }
            }
        }

        try {
            return existingFuture.get();

        } catch (ExecutionException ex) {
            throw (StoreException) ex.getCause();
        } catch (InterruptedException ex) {
            Thread.interrupted();
            throw new StoreException(ex);
        }
    }

    private static class Item {

        final TransactionKey key;
        final TransactionData data;

        Item(TransactionKey key, TransactionData data) {
            this.key = key;
            this.data = data;
        }

    }

    private class CachingTask extends QueueConsumerTask<Item> {

        CachingTask(int queueSize) {
            super("TransactionFetcher-CachingTask", new RequestQueue<>(new ArrayBlockingQueue<>(queueSize)));
        }

        @Override
        public void process(Item item) throws Exception {
            cache.put(item.key, item.data);
            futures.remove(item.key);
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            logger.error("exception caught in CachingTask", ex);
        }

    }

}
