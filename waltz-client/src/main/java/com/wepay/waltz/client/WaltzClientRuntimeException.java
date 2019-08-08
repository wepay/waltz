package com.wepay.waltz.client;

/**
 * A runtime exception class for WaltzClient, wraps an exception that caused the failure.
 */
public class WaltzClientRuntimeException extends RuntimeException {

    /**
     * Class Constructor.
     *
     * @param msg the exception message.
     * @param cause the cause of the failure.
     */
    public WaltzClientRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
