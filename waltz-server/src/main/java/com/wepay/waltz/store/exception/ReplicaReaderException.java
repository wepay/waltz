package com.wepay.waltz.store.exception;

/**
 * This exception is thrown is there is an issue with accessing the
 * {@link com.wepay.waltz.store.internal.ReplicaConnection}.
 */
public class ReplicaReaderException extends ReplicaSessionException {

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public ReplicaReaderException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param ex The cause of the failure.
     */
    public ReplicaReaderException(Throwable ex) {
        super(ex);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     * @param ex The cause of the failure.
     */
    public ReplicaReaderException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
