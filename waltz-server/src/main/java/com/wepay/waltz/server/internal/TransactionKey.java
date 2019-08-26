package com.wepay.waltz.server.internal;

import java.nio.ByteBuffer;

/**
 * Implements the key that is associated with {@link TransactionData}.
 */
public class TransactionKey {

    private static final int PARTITION_ID_SIZE = 4;
    private static final int TRANSACTION_ID_SIZE = 8;
    static final int SERIALIZED_KEY_SIZE = PARTITION_ID_SIZE + TRANSACTION_ID_SIZE;

    public final int partitionId;
    public final long transactionId;

    /**
     * Class constructor.
     * @param partitionId The ID of the partition to which the transaction key is part of.
     * @param transactionId The ID of transaction request received.
     */
    TransactionKey(int partitionId, long transactionId) {
        this.partitionId = partitionId;
        this.transactionId = transactionId;
    }

    /**
     * Writes the partition ID and transaction ID to the given buffer at the offset location provided.
     * @param offset The location where to write the data to in the given buffer.
     * @param buf The buffer to write to.
     */
    public void writeTo(int offset, ByteBuffer buf) {
        buf.putInt(offset, partitionId);
        offset += PARTITION_ID_SIZE;

        buf.putLong(offset, transactionId);
    }

    /**
     * Reads the partition ID and transaction ID from the given buffer.
     * @param offset The location from where to read the partition ID and transaction ID from.
     * @param buf The buffer to read from.
     * @return The transaction key read from the given buffer.
     */
    public static TransactionKey readFrom(int offset, ByteBuffer buf) {
        return new TransactionKey(buf.getInt(offset), buf.getLong(offset + PARTITION_ID_SIZE));
    }

    /**
     * Checks if the partition ID and transaction ID is correct.
     * @param offset The location from where to read the partition ID and transaction ID from.
     * @param buf The buffer to read from.
     * @return True if the partition ID and transaction ID is correct, otherwise returns False.
     */
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
