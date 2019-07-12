package com.wepay.waltz.client;

/**
 * The interface for Waltz client callback methods.
 */
public interface WaltzClientCallbacks {

    /**
     * Returns the current high-water mark of the client application.
     * {@link WaltzClient} calls this method to know which offset to start transaction feeds.
     * @param partitionId
     * @return
     */
    long getClientHighWaterMark(int partitionId);

    /**
     * Applies a committed transaction to the client application.
     * {@link WaltzClient} calls this method to pass a transaction information that is committed to the write ahead log.
     *
     * @param transaction
     */
    void applyTransaction(Transaction transaction);

    /**
     * A method called by the Waltz client when {@link #applyTransaction} throws an exception.
     * @param partitionId
     * @param transactionId
     * @param exception
     */
    void uncaughtException(int partitionId, long transactionId, Throwable exception);

}
