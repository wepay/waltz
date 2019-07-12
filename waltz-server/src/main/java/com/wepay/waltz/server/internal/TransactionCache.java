package com.wepay.waltz.server.internal;

import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.util.Logging;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A transaction cache that stores transaction in a serialized form in a ByteBuffer.
 */
public class TransactionCache {

    private static final Logger logger = Logging.getLogger(TransactionCache.class);
    private static final TransactionKey DUMMY_KEY = new TransactionKey(-1, -1L);
    private static final int MIN_RECORD_SIZE = TransactionKey.SERIALIZED_KEY_SIZE + TransactionData.RECORD_OVERHEAD;

    private final int cacheSize;
    private final ByteBuffer buffer;
    private final ConcurrentHashMap<TransactionKey, Descriptor> index;

    private final int maxSerializedSize;
    private final Meter cacheMissMeter;

    private int startOfFreeSpace;
    private int endOfFreeSpace;

    public TransactionCache(int cacheSize, int maxSerializedSize, boolean directAllocation, Meter cacheMissMeter) {
        if (cacheSize < maxSerializedSize) {
            throw new IllegalArgumentException("cacheSize must be greater than maxSerializedSize");
        }

        this.cacheSize = cacheSize;
        this.buffer = directAllocation ? ByteBuffer.allocateDirect(cacheSize) : ByteBuffer.allocate(cacheSize);
        this.index = new ConcurrentHashMap<>();
        this.maxSerializedSize = maxSerializedSize;
        this.cacheMissMeter = cacheMissMeter;

        this.startOfFreeSpace = 0;
        this.endOfFreeSpace = cacheSize;
    }

    public int size() {
        return cacheSize;
    }

    /**
     * Gets the cached transaction data with the specified partition id and transaction id.
     * @param key transaction data key
     * @return TransactionData, or null if not found.
     */
    public TransactionData get(TransactionKey key) {
        Descriptor descriptor = index.get(key);

        if (descriptor != null) {
            TransactionData data = null;
            int offset = descriptor.offset;
            int length = descriptor.length; // This volatile read ensures the visibility of data.

            if (key.check(offset, buffer)) {
                // The cached item has the correct key. Now read the data.
                data = TransactionData.readFrom(
                    offset + TransactionKey.SERIALIZED_KEY_SIZE,
                    length - TransactionKey.SERIALIZED_KEY_SIZE,
                    buffer
                );
            }

            if (data != null) {
                // Return only when the descriptor is still valid. If the descriptor is invalid, data may be corrupted.
                if (descriptor.isValid()) {
                    return data;
                }
            }
        }

        if (cacheMissMeter != null) {
            cacheMissMeter.mark();
        }

        return null;
    }

    /**
     * Puts a transaction data in the cache. If the serialized size if more than quarter the cache size, the data is not cached.
     * @param key transaction key
     * @param data transaction data
     */
    public void put(TransactionKey key, TransactionData data) {
        int serializedSize = TransactionKey.SERIALIZED_KEY_SIZE + data.serializedSize();

        if (serializedSize > maxSerializedSize) {
            // Data is too big to cache.
            return;
        }

        synchronized (this) {
            Descriptor descriptor = index.get(key);

            if (descriptor != null) {
                if (descriptor.isValid()) {
                    // A valid cached entry exists. Do nothing.
                    return;

                } else {
                    // This shouldn't happen.
                    logger.error("invalid descriptor, removing it");
                    index.remove(key);
                }
            }

            // Reclaim a sufficient space to write the key and data.
            reclaim(serializedSize);

            // Write the key and data.
            key.writeTo(startOfFreeSpace, buffer);
            data.writeTo(startOfFreeSpace + TransactionKey.SERIALIZED_KEY_SIZE, buffer);

            // The creation of a descriptor includes a volatile write. It ensures the visibility of above writes to reader.
            descriptor = new Descriptor(startOfFreeSpace, serializedSize);
            startOfFreeSpace += serializedSize;

            index.put(key, descriptor);
        }
    }

    /**
     * Clear the cache
     */
    public void clear() {
        synchronized (this) {
            for (Descriptor descriptor : index.values()) {
                descriptor.invalidate();
            }
            index.clear();
            endOfFreeSpace = cacheSize;
        }
    }

    /**
     * Reclaims cache space
     * @param targetFreeSize
     */
    private void reclaim(int targetFreeSize) {
        while (endOfFreeSpace - startOfFreeSpace < targetFreeSize) {
            // We need more space.
            int reclaimableSize = cacheSize - endOfFreeSpace;
            if (reclaimableSize > MIN_RECORD_SIZE) {
                // Get the key from the buffer.
                TransactionKey key = TransactionKey.readFrom(endOfFreeSpace, buffer);

                Descriptor descriptor = index.remove(key);
                if (descriptor != null) {
                    endOfFreeSpace += descriptor.length();
                    descriptor.invalidate();

                } else {
                    // The key was not found in the index. It must be the dummy key. The remaining space is unused.
                    endOfFreeSpace = cacheSize;
                }
            } else if (reclaimableSize > 0) {
                // No record can fit in the remaining space. The remaining space is unused. Reclaim it.
                endOfFreeSpace = cacheSize;

            } else {
                // There is no more reclaimable space. Mark the free space unused by writing the dummy key if possible.
                if (endOfFreeSpace - startOfFreeSpace > MIN_RECORD_SIZE) {
                    DUMMY_KEY.writeTo(startOfFreeSpace, buffer);
                }

                // Rewind to the beginning. The next attempt will always be successful.
                endOfFreeSpace = 0;
                startOfFreeSpace = 0;
            }
        }
    }

    private static class Descriptor {

        private final int offset;
        private volatile int length;

        Descriptor(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }

        int length() {
            return length;
        }

        void invalidate() {
            length = -1;
        }

        boolean isValid() {
            return length >= 0;
        }

    }

}
