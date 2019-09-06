package com.wepay.waltz.store.exception;

/**
 * This exception is thrown when a transaction record is not found in the store.
 */
public class TransactionNotFoundException extends StoreException {

    /**
     * Class constructor.
     *
     * @param partitionId The Id of the partition.
     * @param transactionId The Id of the transaction.
     */
    public TransactionNotFoundException(int partitionId, long transactionId) {
        super("partition=" + partitionId + "transactionId=" + transactionId);
    }

}
