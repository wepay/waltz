package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.storage.exception.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class SegmentFileHeader {

    private static final int VERSION = 0;

    public final int version;
    public final long creationTime;
    public final UUID key;
    public final int partitionId;
    public final long firstTransactionId;

    public SegmentFileHeader(UUID key, int partitionId, long firstTransactionId) {
        this(VERSION, System.currentTimeMillis(), key, partitionId, firstTransactionId);
    }

    SegmentFileHeader(int version, long creationTime, UUID key, int partitionId, long firstTransactionId) {
        this.version = version;
        this.creationTime = creationTime;
        this.key = key;
        this.partitionId = partitionId;
        this.firstTransactionId = firstTransactionId;
    }

    void writeTo(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.putInt(VERSION);
        byteBuffer.putLong(System.currentTimeMillis());
        byteBuffer.putLong(key.getMostSignificantBits());
        byteBuffer.putLong(key.getLeastSignificantBits());
        byteBuffer.putInt(partitionId);
        byteBuffer.putLong(firstTransactionId);
    }

    static SegmentFileHeader readFrom(ByteBuffer byteBuffer) throws StorageException, IOException {
        int version = byteBuffer.getInt();
        long creationDate = byteBuffer.getLong();
        UUID key = new UUID(byteBuffer.getLong(), byteBuffer.getLong());
        int partitionId = byteBuffer.getInt();
        long firstTransactionId = byteBuffer.getLong();

        return new SegmentFileHeader(version, creationDate, key, partitionId, firstTransactionId);
    }
}
