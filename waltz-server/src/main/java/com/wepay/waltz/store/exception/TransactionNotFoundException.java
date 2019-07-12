package com.wepay.waltz.store.exception;

/**
 * This exception is thrown when a transaction record is not found in the store.
 */
public class TransactionNotFoundException extends StoreException {

    public TransactionNotFoundException(int partitionId, long transactionId) {
        super("partition=" + partitionId + "transactionId=" + transactionId);
    }

}
