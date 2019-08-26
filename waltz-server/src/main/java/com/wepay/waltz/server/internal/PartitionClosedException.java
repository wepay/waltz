package com.wepay.waltz.server.internal;

/**
 * This exception is thrown if a partition client sends a request to a partition which is closed.
 */
public class PartitionClosedException extends Exception {

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public PartitionClosedException(String msg) {
        super(msg);
    }

}
