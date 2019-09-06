package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if the {@link com.wepay.waltz.store.internal.ReplicaWriter}
 * trying to access is closed.
 */
public class ReplicaWriterException extends ReplicaSessionException {

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public ReplicaWriterException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param ex The cause of the failure.
     */
    public ReplicaWriterException(Throwable ex) {
        super(ex);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     * @param ex The cause of the failure.
     */
    public ReplicaWriterException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
