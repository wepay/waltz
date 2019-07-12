package com.wepay.waltz.store.internal.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class StoreParams {

    private static final byte VERSION = 1;

    public final UUID key;
    public final int numPartitions;

    public StoreParams(UUID key, int numPartitions) {
        this.key = key;
        this.numPartitions = numPartitions;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeLong(key.getMostSignificantBits());
        out.writeLong(key.getLeastSignificantBits());
        out.writeInt(numPartitions);
    }

    public static StoreParams readFrom(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException(String.format("unsupported version. expected %d but got %d", VERSION, version));
        }

        UUID key = new UUID(in.readLong(), in.readLong());
        int numPartitions = in.readInt();

        return new StoreParams(key, numPartitions);
    }

}
