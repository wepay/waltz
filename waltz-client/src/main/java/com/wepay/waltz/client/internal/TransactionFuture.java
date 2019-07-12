package com.wepay.waltz.client.internal;

import com.wepay.waltz.common.message.ReqId;

import java.util.concurrent.CompletableFuture;

public class TransactionFuture extends CompletableFuture<Boolean> {

    public final ReqId reqId;

    private boolean flushed = false;

    public TransactionFuture(ReqId reqId) {
        this.reqId = reqId;
    }

    /**
     * Mark this TransactionFuture flushed. This is called when this future is removed from {@code TransactionMonitor}.
     */
    void flushed() {
        synchronized (this) {
            flushed = true;
            notifyAll();
        }
    }

    boolean isFlushed() {
        synchronized (this) {
            return flushed;
        }
    }

    /**
     * Wait until this TransactionFuture is flushed from {@code TransactionMonitor}.
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
