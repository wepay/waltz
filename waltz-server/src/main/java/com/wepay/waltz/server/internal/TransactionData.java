package com.wepay.waltz.server.internal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.util.Arrays;

@SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "internal class")
public class TransactionData {

    private static final int DATA_LENGTH_SIZE = 4;
    private static final int CHECKSUM_SIZE = 4;

    static final int RECORD_OVERHEAD = DATA_LENGTH_SIZE + CHECKSUM_SIZE;

    public final byte[] data;
    public final int checksum;

    TransactionData(byte[] data, int checksum) {
        this.data = data;
        this.checksum = checksum;
    }

    public int serializedSize() {
        return RECORD_OVERHEAD + data.length;
    }

    public void writeTo(int offset, ByteBuffer buf) {
        buf.putInt(offset, data.length);
        offset += DATA_LENGTH_SIZE;

        for (byte oneByte : data) {
            buf.put(offset++, oneByte);
        }

        buf.putInt(offset, checksum);
    }

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
