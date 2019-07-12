package com.wepay.waltz.client;

/**
 * WaltzClientRuntimeException is a runtime exception class for WaltzClient.
 * It wraps an exception that caused the failure.
 */
public class WaltzClientRuntimeException extends RuntimeException {

    public WaltzClientRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
