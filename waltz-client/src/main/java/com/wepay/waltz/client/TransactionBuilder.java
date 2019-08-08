package com.wepay.waltz.client;

import java.util.List;

/**
 * An interface for building transaction requests to send to Waltz cluster.
 */
public interface TransactionBuilder {

    /**
     * Sets 32-bit transaction header information.
     *
     * @param header the header information
     */
    void setHeader(int header);

    /**
     * Sets the transaction data.
     *
     * @param transactionData the transaction data of type {@code T}.
     * @param serializer the {@code Serializer} to serialize transaction data.
     * @param <T> the type of the transaction data.
     */
    <T> void setTransactionData(T transactionData, Serializer<T> serializer);

    /**
     * Sets optimistic write locks.
     *
     * @param partitionLocalLocks a list of {@link PartitionLocalLock}
     */
    void setWriteLocks(List<PartitionLocalLock> partitionLocalLocks);

    /**
     * Sets optimistic read locks.
     *
     * @param partitionLocalLocks a list of {@link PartitionLocalLock}
     */
    void setReadLocks(List<PartitionLocalLock> partitionLocalLocks);

}
