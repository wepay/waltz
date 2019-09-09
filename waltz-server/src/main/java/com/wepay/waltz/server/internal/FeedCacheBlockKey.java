package com.wepay.waltz.server.internal;

final class FeedCacheBlockKey {

    static final int SIZE = 4 + 8;
    private static final long BLOCK_ID_MASK = 0xFFFFFFFFFFFFFFC0L;
    private static final int PARTITION_ID_SHIFT = 12345;

    private final int partitionId;
    private final long blockId;

    private FeedCacheBlockKey(int partitionId, long blockId) {
        this.partitionId = partitionId;
        this.blockId = blockId;
    }

    static FeedCacheBlockKey get(int partitionId, long transactionId) {
        return new FeedCacheBlockKey(partitionId, transactionId & BLOCK_ID_MASK);
    }

    long firstTransactionId() {
        return blockId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode((partitionId + PARTITION_ID_SHIFT) * blockId);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FeedCacheBlockKey
            && ((FeedCacheBlockKey) obj).partitionId == partitionId
            && ((FeedCacheBlockKey) obj).blockId == blockId;
    }

}
