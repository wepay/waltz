package com.wepay.waltz.storage.server.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

class ControlFileHeader {

    static final int VERSION = 0;

    final int version;
    final long creationTime;
    final UUID key;
    final int numPartitions;

    ControlFileHeader(int version, long creationTime, UUID key, int numPartitions) {
        this.version = version;
        this.creationTime = creationTime;
        this.key = key;
        this.numPartitions = numPartitions;
    }

    void writeTo(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.putInt(VERSION);
        byteBuffer.putLong(System.currentTimeMillis());
        byteBuffer.putLong(key.getMostSignificantBits());
        byteBuffer.putLong(key.getLeastSignificantBits());
        byteBuffer.putInt(numPartitions);
    }

    static ControlFileHeader readFrom(ByteBuffer byteBuffer) throws IOException {
        return new ControlFileHeader(
            byteBuffer.getInt(),
            byteBuffer.getLong(),
            new UUID(byteBuffer.getLong(), byteBuffer.getLong()),
            byteBuffer.getInt()
        );
    }

}
