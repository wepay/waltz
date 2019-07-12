package com.wepay.waltz.server.internal;

import java.util.Arrays;
import java.util.BitSet;

public class Locks {

    private static final int MULTIPLIER = 1103515245;
    private static final int ADDER = 12345;
    private static final int MASK = 0x3FFFFFFF;

    private final long[] highWaterMarks;
    private final BitSet locks;
    private final int numHashFuncs;

    private int numActiveLocks = 0;

    public Locks(int size, int numHashFuncs, long defaultHighWaterMark) {
        this.highWaterMarks = new long[size];
        Arrays.fill(this.highWaterMarks, defaultHighWaterMark);
        this.locks = new BitSet(size);
        this.numHashFuncs = numHashFuncs;
    }

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

    public void commit(LockRequest request, long transactionId) {
        for (int hash : request.writeLocks) {
            for (int i = 0; i < numHashFuncs; i++) {
                // linear congruential generator
                hash = nextHash(hash);
                highWaterMarks[index(hash)] = transactionId;
            }
        }
    }

    public void reset(long defaultHighWaterMark) {
        synchronized (locks) {
            Arrays.fill(highWaterMarks, defaultHighWaterMark);
            locks.clear();
            numActiveLocks = 0;
        }
    }

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

    public static LockRequest createRequest(int[] writeLocks, int[] readLocks) {
        return new LockRequest(writeLocks, readLocks);
    }

    public static class LockRequest {
        private final int[] writeLocks;
        private final int[] readLocks;

        LockRequest(int[] writeLocks, int[] readLocks) {
            this.writeLocks = writeLocks;
            this.readLocks = readLocks;
        }
    }

}
