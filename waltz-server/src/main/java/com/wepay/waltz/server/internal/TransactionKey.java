package com.wepay.waltz.server.internal;

import java.nio.ByteBuffer;

public class TransactionKey {

    private static final int PARTITION_ID_SIZE = 4;
    private static final int TRANSACTION_ID_SIZE = 8;
    static final int SERIALIZED_KEY_SIZE = PARTITION_ID_SIZE + TRANSACTION_ID_SIZE;

    public final int partitionId;
    public final long transactionId;

    TransactionKey(int partitionId, long transactionId) {
        this.partitionId = partitionId;
        this.transactionId = transactionId;
    }

    public void writeTo(int offset, ByteBuffer buf) {
        buf.putInt(offset, partitionId);
        offset += PARTITION_ID_SIZE;

        buf.putLong(offset, transactionId);
    }

    public static TransactionKey readFrom(int offset, ByteBuffer buf) {
        return new TransactionKey(buf.getInt(offset), buf.getLong(offset + PARTITION_ID_SIZE));
    }

    public boolean check(int offset, ByteBuffer buf) {
        return partitionId == buf.getInt(offset) && transactionId == buf.getLong(offset + PARTITION_ID_SIZE);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(transactionId ^ ((long) partitionId << 16));
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TransactionKey
            && this.partitionId == ((TransactionKey) obj).partitionId
            && this.transactionId == ((TransactionKey) obj).transactionId;
    }

}
