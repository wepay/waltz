package com.wepay.waltz.storage.server.internal;

/**
 * Read-only summary representation of a {@link PartitionInfo} class.
 */
public final class PartitionInfoSnapshot {
    public final int partitionId;
    public final long sessionId;
    public final long lowWaterMark;
    public final long localLowWaterMark;
    public final boolean isAssigned;
    public final boolean isAvailable;

    public PartitionInfoSnapshot(int partitionId, long sessionId, long lowWaterMark, long localLowWaterMark, int flags) {
        this.partitionId = partitionId;
        this.sessionId = sessionId;
        this.lowWaterMark = lowWaterMark;
        this.localLowWaterMark = localLowWaterMark;
        this.isAssigned = PartitionInfo.Flags.isFlagSet(flags, PartitionInfo.Flags.PARTITION_IS_ASSIGNED);
        this.isAvailable = PartitionInfo.Flags.isFlagSet(flags, PartitionInfo.Flags.PARTITION_IS_AVAILABLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof PartitionInfoSnapshot) {
            PartitionInfoSnapshot other = (PartitionInfoSnapshot) o;
            return partitionId == other.partitionId
                    && sessionId == other.sessionId
                    && lowWaterMark == other.lowWaterMark
                    && localLowWaterMark == other.localLowWaterMark
                    && isAssigned == other.isAssigned
                    && isAvailable == other.isAvailable;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return partitionId ^ Long.hashCode(sessionId) ^ Long.hashCode(lowWaterMark) ^ Long.hashCode(localLowWaterMark) ^ Boolean.hashCode(isAssigned) ^ Boolean.hashCode(isAvailable);
    }
}
