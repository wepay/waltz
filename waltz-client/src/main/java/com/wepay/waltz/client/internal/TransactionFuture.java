package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.common.message.ReqId;

import java.util.concurrent.CompletableFuture;

/**
 * A CompletableFuture which is explicitly completed when transaction response is received from a Waltz server.
 */
public class TransactionFuture extends CompletableFuture<Boolean> {

    public final ReqId reqId;

    private TransactionContext transactionContext;
    private boolean flushed = false;

    /**
     * Class Constructor.
     *
     * @param reqId the id of the request.
     * @param transactionContext the transaction context
     */
    public TransactionFuture(ReqId reqId, TransactionContext transactionContext) {
        this.reqId = reqId;
        this.transactionContext = transactionContext;
    }

    /**
     * Returns the TransactionContext associate with this TransactionFuture.
     * This returns null after the TransactionFuture is completed.
     */
    TransactionContext getTransactionContext() {
        synchronized (this) {
            return transactionContext;
        }
    }

    /**
     * Marks this TransactionFuture flushed. This is called when this future is removed from {@link TransactionMonitor}.
     */
    void flushed() {
        synchronized (this) {
            flushed = true;
            notifyAll();
        }
    }

    /**
     * @return {@code true} if it is flushed. {@code false}, otherwise.
     */
    boolean isFlushed() {
        synchronized (this) {
            return flushed;
        }
    }

    /**
     * Waits until this TransactionFuture is flushed from {@link TransactionMonitor}.
     */
    void awaitFlush() {
        synchronized (this) {
            while (!flushed) {
                try {
                    wait();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        }
    }

    @Override
    public boolean complete(Boolean value) {
        synchronized (this) {
            transactionContext = null;
            return super.complete(value);
        }
    }

    @Override
    public boolean completeExceptionally(Throwable exception) {
        synchronized (this) {
            transactionContext = null;
            return super.completeExceptionally(exception);
        }
    }

}
