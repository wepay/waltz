package com.wepay.waltz.common.message;

import com.wepay.riff.network.MessageAttributeReader;
import com.wepay.riff.network.MessageAttributeWriter;

public class ReqId {

    public final long mostSigBits;
    public final long leastSigBits;

    private static final long INT_TO_LONG = 0xFFFFFFFFL;
    private static final long BYTE_TO_LONG = 0xFFL;

    public ReqId(int clientId, int generation, int partitionId, int seqNum) {
        this(
            (((long) clientId) << 32) | (((long) generation) & INT_TO_LONG),
            (((long) partitionId) << 32) | (((long) seqNum) & INT_TO_LONG)
        );
    }

    public ReqId(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }

    public int clientId() {
        return (int) (mostSigBits >>> 32);
    }

    public int generation() {
        return (int) mostSigBits;
    }

    public int partitionId() {
        return (int) (leastSigBits >>> 32);
    }

    public int seqNum() {
        return (int) leastSigBits;
    }

    public int hashCode() {
        return (int) ((mostSigBits ^ leastSigBits) % INT_TO_LONG);
    }

    public boolean eq(ReqId other) {
        return other != null && mostSigBits == other.mostSigBits && leastSigBits == other.leastSigBits;
    }

    @Override
    public boolean equals(Object obj) {
        try {
            return eq((ReqId) obj);
        } catch (ClassCastException ex) {
            return false;
        }
    }

    public static ReqId readFrom(MessageAttributeReader reader) {
        return new ReqId(reader.readLong(), reader.readLong());
    }

    public void writeTo(MessageAttributeWriter writer) {
        writer.writeLong(mostSigBits);
        writer.writeLong(leastSigBits);
    }

    public static ReqId readFrom(byte[] buf, int offset) {
        return new ReqId(readLongFrom(buf, offset), readLongFrom(buf, offset + 8));
    }

    public void writeTo(byte[] buf, int offset) {
        writeLongTo(buf, offset, mostSigBits);
        writeLongTo(buf, offset + 8, leastSigBits);
    }

    private static long readLongFrom(byte[] buf, int offset) {
        return ((long) buf[offset] & BYTE_TO_LONG) << 56
            | ((long) buf[offset + 1] & BYTE_TO_LONG) << 48
            | ((long) buf[offset + 2] & BYTE_TO_LONG) << 40
            | ((long) buf[offset + 3] & BYTE_TO_LONG) << 32
            | ((long) buf[offset + 4] & BYTE_TO_LONG) << 24
            | ((long) buf[offset + 5] & BYTE_TO_LONG) << 16
            | ((long) buf[offset + 6] & BYTE_TO_LONG) << 8
            | (long) buf[offset + 7] & BYTE_TO_LONG;
    }

    private void writeLongTo(byte[] buf, int offset, long value) {
        buf[offset] = (byte) ((int) (value >>> 56));
        buf[offset + 1] = (byte) ((int) (value >>> 48));
        buf[offset + 2] = (byte) ((int) (value >>> 40));
        buf[offset + 3] = (byte) ((int) (value >>> 32));
        buf[offset + 4] = (byte) ((int) (value >>> 24));
        buf[offset + 5] = (byte) ((int) (value >>> 16));
        buf[offset + 6] = (byte) ((int) (value >>> 8));
        buf[offset + 7] = (byte) ((int) value);
    }

    @Override
    public String toString() {
        return "ReqId(c:" + clientId() + ",g:" + generation() + ",p:" + partitionId() + ",s:" + seqNum() + ")";
    }

}
