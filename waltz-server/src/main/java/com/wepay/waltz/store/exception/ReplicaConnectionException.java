package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if {@link com.wepay.waltz.store.internal.ReplicaReader}'s connectionFuture
 * fails with an exception while trying to access {@link com.wepay.waltz.store.internal.ReplicaConnection}.
 */
public class ReplicaConnectionException extends ReplicaSessionException {

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public ReplicaConnectionException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     * @param ex The cause of the failure.
     */
    public ReplicaConnectionException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
