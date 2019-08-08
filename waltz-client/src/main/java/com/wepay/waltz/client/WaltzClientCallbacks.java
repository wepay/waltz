package com.wepay.waltz.client;

/**
 * The interface for Waltz client callback methods.
 */
public interface WaltzClientCallbacks {

    /**
     * Returns the current high-water mark of the client application.
     * {@link WaltzClient} calls this method to know which offset to start transaction feeds from.
     *
     * @param partitionId the partition id to get client high-water mark of.
     * @return client high-water mark.
     */
    long getClientHighWaterMark(int partitionId);

    /**
     * Applies a committed transaction to the client application.
     * {@link WaltzClient} calls this method to pass a transaction information that is committed to the write ahead log.
     *
     * @param transaction a committed transaction.
     */
    void applyTransaction(Transaction transaction);

    /**
     * A method called by the Waltz client when {@link #applyTransaction(Transaction)} throws an exception.
     *
     * @param partitionId the partition id of the transaction.
     * @param transactionId the id of the transaction.
     * @param exception thrown exception by {@link #applyTransaction(Transaction)}.
     */
    void uncaughtException(int partitionId, long transactionId, Throwable exception);

}
