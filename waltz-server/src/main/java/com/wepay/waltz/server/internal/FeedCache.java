package com.wepay.waltz.server.internal;

import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.util.Logging;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>
 * This class is the cache for feed data.
 * </p>
 * <p>
 * It uses two levels of caching, the partition level and the instance level.
 * The cache is divided into blocks. Each block holds up to 64 transaction feed data.
 * The cache blocks move between two levels.
 * The total number of blocks is determined at start-up.
 * An instance has a shared pool of blocks, and each partition has a partition local pool of blocks.
 * A partition checks out a block from the shared pool and put it in its local pool.
 * If the pool size gets too big, the partition checks in the oldest block back into the shared pool.
 * The shared pool works like an extension of a local pool. A block is still valid after it is put back in the shared pool.
 * A partition can bring it back to the local pool when necessary. So, a high traffic partition utilizes more blocks effectively.
 * </p>
 * <p>
 * It is assumed that a majority of cache accesses are satisfied with the local pool.
 * So there won't be excessive contentions on the shared pool.
 * </p>
 * <p>
 * Both types of pools use the FIFO replacement policy.
 * The behavior of two level caching as a whole is more complicated.
 * It is slightly closer to LRU policy or second chance replacement policy than pure FIFO policy.
 * </p>
 * <p>
 * The local pool size is basically (the total number of blocks) / (the number of partition assigned + 1),
 * but not less than {@code MIN_PARTITION_SIZE} and not greater than {@code MAX_PARTITION_SIZE}.
 * </p>
 */
public class FeedCache {

    static final int BLOCK_SIZE = FeedCacheBlock.SIZE + FeedCacheBlockKey.SIZE;
    private static final Logger logger = Logging.getLogger(FeedCache.class);
    private static final int MIN_PARTITION_SIZE = 2;
    private static final int MAX_PARTITION_SIZE = 10000;

    private final LinkedHashMap<FeedCacheBlockKey, FeedCacheBlock> sharedPool;
    private final Map<Integer, FeedCachePartition> partitions;
    private final int totalNumBlocks;
    private final Meter cacheMissMeter;

    private int partitionSize;
    private boolean running = true;

    /**
     * Class constructor. Initializes the shared pool.
     * @param size The size of the Feed cache.
     * @param cacheMissMeter Metric to track cache miss.
     */
    public FeedCache(int size, Meter cacheMissMeter) {
        this.cacheMissMeter = cacheMissMeter;
        this.sharedPool = new LinkedHashMap<>();
        this.partitions = new HashMap<>();
        this.totalNumBlocks = size / BLOCK_SIZE;

        for (int i = 0; i < this.totalNumBlocks; i++) {
            this.sharedPool.put(FeedCacheBlockKey.get(-1, i * FeedCacheBlock.NUM_TRANSACTIONS), new FeedCacheBlock());
        }
    }

    /**
     * Closes the feed cache.
     */
    public void close() {
        synchronized (sharedPool) {
            running = false;
            sharedPool.notifyAll();
        }
    }

    /**
     * Returns a cache partition.
     * @param partitionId The ID of the partition whose feed cache is requested.
     * @return Feed cache for a given partition.
     */
    public FeedCachePartition getPartition(Integer partitionId) {
        synchronized (partitions) {
            FeedCachePartition partition = partitions.get(partitionId);
            if (partition == null) {
                partition = new FeedCachePartition(partitionId, this);
                partitions.put(partitionId, partition);
                adjustPartitionSize();
            }
            partition.open();
            return partition;
        }
    }

    /**
     * Remove a partition. This method should be called only from FeedCachePartition.
     * @param partitionId The ID of the partition that has to be removed.
     */
    void removePartition(Integer partitionId) {
        synchronized (partitions) {
            partitions.remove(partitionId);
            adjustPartitionSize();
        }
    }

    /**
     * Returns the number of active partitions.
     * @return the number of active partitions.
     */
    int getNumPartitions() {
        synchronized (partitions) {
            return partitions.size();
        }
    }

    /**
     * Returns local pool size of a partition.
     * @return local pool size of a partition.
     */
    int getPartitionSize() {
        synchronized (partitions) {
            return partitionSize;
        }
    }

    /**
     * Returns the number of blocks to the shared pool.
     * @return the number of blocks.
     */
    int getSharedPoolSize() {
        synchronized (sharedPool) {
            return sharedPool.size();
        }
    }

    /**
     * Checks out a cache block from the shared pool.
     * @param key The key of a specific block from the shared pool.
     * @return a cache block.
     */
    FeedCacheBlock checkOut(FeedCacheBlockKey key) {
        synchronized (sharedPool) {
            FeedCacheBlock block = sharedPool.remove(key);

            if (block != null) {
                return block;

            } else {
                while (running) {
                    Map.Entry<?, FeedCacheBlock> entry = removeOldestEntry(sharedPool);
                    if (entry != null) {
                        block = entry.getValue();
                        block.reset(key);
                        return block;

                    } else {
                        logger.warn("unable to checkout a cache block. you may want to increase feed cache size");
                        try {
                            sharedPool.wait();

                        } catch (InterruptedException ex) {
                            Thread.interrupted();
                        }
                    }
                }
                return null;
            }
        }
    }

    /**
     * Checks in a block into the shared pool.
     * @param key The key of a block to be added to the shared pool.
     * @param block The block associated with the given key.
     */
    void checkIn(FeedCacheBlockKey key, FeedCacheBlock block) {
        synchronized (sharedPool) {
            sharedPool.put(key, block);
            sharedPool.notifyAll();
        }
    }

    /**
     * Checks in a map of blocks into the shared pool.
     * @param map A map of blocks (FeedCacheBlockKey -> FeedCacheBlock)
     */
    void checkInAll(Map<FeedCacheBlockKey, FeedCacheBlock> map) {
        synchronized (sharedPool) {
            map.forEach(sharedPool::put);
            sharedPool.notifyAll();
        }
    }

    /**
     * Marks a cache miss.
     */
    void markCacheMiss() {
        if (cacheMissMeter != null) {
            cacheMissMeter.mark();
        }
    }

    private void adjustPartitionSize() {
        partitionSize = Math.min(Math.max(totalNumBlocks / (partitions.size() + 1), MIN_PARTITION_SIZE), MAX_PARTITION_SIZE);

        for (FeedCachePartition partition : partitions.values()) {
            partition.setMaxNumBlocks(partitionSize);
        }
    }

    private static <K, V> Map.Entry<K, V> removeOldestEntry(LinkedHashMap<K, V> map) {
        Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<K, V> entry = iterator.next();
            iterator.remove();
            return entry;

        } else {
            return null;
        }
    }

}
