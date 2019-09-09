package com.wepay.waltz.client;

/**
 * The abstract class of the transaction context.
 */
public abstract class TransactionContext {

    public final long creationTime;

    /**
     * Class Constructor.
     */
    public TransactionContext() {
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * Returns the partition id for this transaction.
     *
     * @param numPartitions the number of partitions.
     * @return the partitionId.
     */
    public abstract int partitionId(int numPartitions);

    /**
     * <p>
     * Executes the transaction. An application must implement this method.
     * </p><p>
     * The application sets the header and the data of the transaction using the builder, and optionally sets locks.
     * When this returns true, the Waltz client builds the transaction from the builder and sends an append request
     * to a Waltz server. If the client failed to send the request, it will call this method to execute the transaction
     * again.
     * </p><P>
     * If the application finds that the transaction must be ignored, this call must return false.
     * </P><P>
     * If an exception is thrown by this method, the client will call {@link TransactionContext#onException(Throwable)}.
     * </P>
     *
     * @param builder TransactionBuilder.
     * @return {@code true} if the transaction should be submitted, {@code false} if the transaction should be ignored.
     */
    public abstract boolean execute(TransactionBuilder builder);

    /**
     * A method that is called on completion of this transaction context that did not fail due to expiration or exception.
     * After this call, no retry will be attempted by the Waltz client.
     * The {@code result} parameter is {@code true} if the transaction is successfully appended to Waltz log,
     * otherwise {@code false}, i.e., the transaction is ignored.
     *
     * @param result {@code true} if the transaction is successfully appended to Waltz log, otherwise {@code false}.
     */
    public void onCompletion(boolean result) {
    }

    /**
     * A method that is called on exception.
     * After this call, no retry will be attempted by the Waltz client.
     *
     * @param ex The exception that was thrown.
     */
    public void onException(Throwable ex) {
    }

}
