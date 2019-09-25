package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.common.message.ReqId;

import java.util.concurrent.CompletableFuture;

/**
 * A CompletableFuture which is explicitly completed when transaction response is received from a Waltz server.
 */
public class TransactionFuture extends CompletableFuture<Boolean> {

    public final ReqId reqId;
    public final TransactionContext transactionContext;

    private boolean flushed = false;

    /**
     * Class Constructor.
     *
     * @param reqId the id of the request.
     */
    public TransactionFuture(ReqId reqId, TransactionContext transactionContext) {
        this.reqId = reqId;
        this.transactionContext = transactionContext;
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

}
