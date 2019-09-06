package com.wepay.waltz.store.internal.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * This class implements store parameters.
 */
public class StoreParams {

    private static final byte VERSION = 1;

    public final UUID key;
    public final int numPartitions;

    /**
     * Class constructor.
     * @param key The cluster key.
     * @param numPartitions The total number of partitions in the cluster.
     */
    public StoreParams(UUID key, int numPartitions) {
        this.key = key;
        this.numPartitions = numPartitions;
    }

    /**
     * Writes store parameters via the {@link DataOutput} provided.
     * @param out The interface that converts the data to a series of bytes.
     * @throws IOException thrown if the write fails.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeLong(key.getMostSignificantBits());
        out.writeLong(key.getLeastSignificantBits());
        out.writeInt(numPartitions);
    }

    /**
     * Reads store parameters via the {@link DataInput} provided.
     * @param in The interface that reads bytes from a binary stream and converts it
     *        to the data of required type.
     * @return Returns {@code StoreParams}.
     * @throws IOException thrown if the read fails.
     */
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
