package com.wepay.waltz.exception;

/**
 * SubCommandFailedException is a runtime exception class for SubCommandCli.
 * It contains the message describing the cause of exception.
 */
public class SubCommandFailedException extends RuntimeException {

    public SubCommandFailedException(String msg) {
        super(msg);
    }

    public SubCommandFailedException(Throwable cause) {
        super(cause);
    }

}
