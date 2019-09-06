package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if the {@link com.wepay.waltz.store.internal.ReplicaSession}
 * trying to access is closed.
 */
public class ReplicaSessionException extends Exception {

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public ReplicaSessionException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param ex The cause of the failure.
     */
    public ReplicaSessionException(Throwable ex) {
        super(ex);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     * @param ex The cause of the failure.
     */
    public ReplicaSessionException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
