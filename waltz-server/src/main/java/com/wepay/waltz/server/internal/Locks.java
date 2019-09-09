package com.wepay.waltz.server.internal;

import java.util.Arrays;
import java.util.BitSet;

/**
 * This class represents the lock table that can contain multiple read and write locks for a {@link Partition} in the
 * {@link com.wepay.waltz.server.WaltzServer}. It uses a probabilistic approach similar to Bloom Filter to determine the
 * estimated transaction ID of the last successful transaction for the given lock ID. The estimated transaction ID is
 * guaranteed to be equal to or greater than the true transaction ID.
 */
public class Locks {

    private static final int MULTIPLIER = 1103515245;
    private static final int ADDER = 12345;
    private static final int MASK = 0x3FFFFFFF;

    private final long[] highWaterMarks;
    private final BitSet locks;
    private final int numHashFuncs;

    private int numActiveLocks = 0;

    /**
     * Class contsructor.
     * @param size The size of the optimistic lock table.
     * @param numHashFuncs The number of hash functions to use inorder to reduce the false positives.
     * @param defaultHighWaterMark The default high-water mark to set initially.
     */
    public Locks(int size, int numHashFuncs, long defaultHighWaterMark) {
        this.highWaterMarks = new long[size];
        Arrays.fill(this.highWaterMarks, defaultHighWaterMark);
        this.locks = new BitSet(size);
        this.numHashFuncs = numHashFuncs;
    }

    /**
     * Returns True if the locks doesn't overlap with the locks that are currently in use, otherwise returns False.
     * Also acquires the write locks while returning True.
     * @param request List of write locks and read locks to use.
     * @return True if the locks doesn't overlap with the locks that are currently in use, otherwise returns False.
     */
    public boolean begin(LockRequest request) {
        synchronized (locks) {
            if (begin(request.writeLocks) && begin(request.readLocks)) {
                // Mark entries for write locks
                mark(request.writeLocks);
                numActiveLocks++;
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean begin(int[] lockRequest) {
        for (int hash : lockRequest) {
            for (int i = 0; i < numHashFuncs; i++) {
                // linear congruential generator
                hash = nextHash(hash);
                int index = index(hash);
                if (locks.get(index)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Releases the write locks.
     * @param request List of write locks to be released.
     */
    public void end(LockRequest request) {
        synchronized (locks) {
            unmark(request.writeLocks);
            numActiveLocks--;
        }
    }

    private void mark(int[] lockRequest) {
        for (int hash : lockRequest) {
            for (int i = 0; i < numHashFuncs; i++) {
                // linear congruential generator
                hash = nextHash(hash);
                locks.set(index(hash), true);
            }
        }
    }

    private void unmark(int[] lockRequest) {
        for (int hash : lockRequest) {
            for (int i = 0; i < numHashFuncs; i++) {
                // linear congruential generator
                hash = nextHash(hash);
                locks.set(index(hash), false);
            }
        }
    }

    /**
     * Returns the estimated transaction ID of the last successful transaction for the given lock.
     * @param request Lock for which last successful transaction ID is to be determined.
     * @return the estimated transaction ID of the last successful transaction for the given lock.
     */
    public long getLockHighWaterMark(LockRequest request) {
        return Math.max(getLockHighWaterMark(request.writeLocks), getLockHighWaterMark(request.readLocks));
    }

    private long getLockHighWaterMark(int[] lockRequest) {
        long maxHighWaterMark = -1L;

        for (int hash : lockRequest) {
            long minHighWaterMark = Long.MAX_VALUE;

            for (int i = 0; i < numHashFuncs; i++) {
                // linear congruential generator
                hash = nextHash(hash);
                minHighWaterMark = Math.min(minHighWaterMark, highWaterMarks[index(hash)]);
            }

            if (minHighWaterMark > maxHighWaterMark) {
                maxHighWaterMark = minHighWaterMark;
            }
        }

        return maxHighWaterMark;
    }

    /**
     * Updates the transaction ID in the locks table.
     * @param request The write locks for which the transaction ID has to be updated.
     * @param transactionId The new transaction ID of the lock.
     */
    public void commit(LockRequest request, long transactionId) {
        for (int hash : request.writeLocks) {
            for (int i = 0; i < numHashFuncs; i++) {
                // linear congruential generator
                hash = nextHash(hash);
                highWaterMarks[index(hash)] = transactionId;
            }
        }
    }

    /**
     * Clear all locks and reset their high-water mark to the default high-water mark provided.
     * @param defaultHighWaterMark The new default high-water mark.
     */
    public void reset(long defaultHighWaterMark) {
        synchronized (locks) {
            Arrays.fill(highWaterMarks, defaultHighWaterMark);
            locks.clear();
            numActiveLocks = 0;
        }
    }

    /**
     * Returns number of active locks.
     * @return number of active locks.
     */
    public int numActiveLocks() {
        synchronized (locks) {
            return numActiveLocks;
        }
    }

    private static int nextHash(int hash) {
        // linear congruential generator
        return hash * MULTIPLIER + ADDER;
    }

    private int index(int hash) {
        return (hash & MASK) % highWaterMarks.length;
    }

    /**
     * Returns the lock request which contains an array of write locks and read locks.
     * @param writeLocks Array of write locks to use.
     * @param readLocks Array of read locks to use.
     * @return the lock request which contains an array of write locks and read locks.
     */
    public static LockRequest createRequest(int[] writeLocks, int[] readLocks) {
        return new LockRequest(writeLocks, readLocks);
    }

    /**
     * Contains an array of write locks and read locks to use.
     */
    public static class LockRequest {
        private final int[] writeLocks;
        private final int[] readLocks;

        LockRequest(int[] writeLocks, int[] readLocks) {
            this.writeLocks = writeLocks;
            this.readLocks = readLocks;
        }
    }

}
