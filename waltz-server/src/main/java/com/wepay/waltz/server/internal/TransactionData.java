package com.wepay.waltz.server.internal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The transaction data that is written/read to/from a partition.
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "internal class")
public class TransactionData {

    private static final int DATA_LENGTH_SIZE = 4;
    private static final int CHECKSUM_SIZE = 4;

    static final int RECORD_OVERHEAD = DATA_LENGTH_SIZE + CHECKSUM_SIZE;

    public final byte[] data;
    public final int checksum;

    /**
     * Class constructor.
     * @param data The data to be written to a partition.
     * @param checksum The checksum of the data received from the client.
     */
    TransactionData(byte[] data, int checksum) {
        this.data = data;
        this.checksum = checksum;
    }

    /**
     * Returns the size of the data (header + payload).
     * @return the size of the data (header + payload).
     */
    public int serializedSize() {
        return RECORD_OVERHEAD + data.length;
    }

    /**
     * Writes the data to a buffer.
     * @param offset The location where to append the data to in the given buffer.
     * @param buf The buffer to write to.
     */
    public void writeTo(int offset, ByteBuffer buf) {
        buf.putInt(offset, data.length);
        offset += DATA_LENGTH_SIZE;

        for (byte oneByte : data) {
            buf.put(offset++, oneByte);
        }

        buf.putInt(offset, checksum);
    }

    /**
     * Reads the data from the buffer.
     * @param offset The location from where to read the data from.
     * @param length The length of the data to read.
     * @param buf The buffer to read from.
     * @return The transaction data read from the given buffer.
     */
    public static TransactionData readFrom(int offset, int length, ByteBuffer buf) {
        if (length < RECORD_OVERHEAD) {
            return null;
        }

        int dataLen = buf.getInt(offset);
        offset += DATA_LENGTH_SIZE;

        if (dataLen < 0 || length < RECORD_OVERHEAD + dataLen) {
            return null;
        }

        byte[] data = new byte[dataLen];
        for (int i = 0; i < dataLen; i++) {
            data[i] = buf.get(offset++);
        }

        int checksum = buf.getInt(offset);

        return new TransactionData(data, checksum);
    }

    @Override
    public int hashCode() {
        return checksum;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TransactionData
            && this.checksum == ((TransactionData) obj).checksum
            && Arrays.equals(this.data, ((TransactionData) obj).data);
    }

}
