package com.wepay.waltz.client;

import java.util.List;

public interface TransactionBuilder {

    /**
     * Sets 32-bit transaction header information.
     * @param header
     */
    void setHeader(int header);

    /**
     * Sets the transaction data
     * @param transactionData
     * @param serializer
     * @param <T>
     */
    <T> void setTransactionData(T transactionData, Serializer<T> serializer);

    /**
     * Sets optimistic write locks
     * @param partitionLocalLocks
     */
    void setWriteLocks(List<PartitionLocalLock> partitionLocalLocks);

    /**
     * Sets optimistic read locks
     * @param partitionLocalLocks
     */
    void setReadLocks(List<PartitionLocalLock> partitionLocalLocks);

}


