package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if recovery fails. This might happen if
 * we are trying to start a recovery that has already been started.
 */
public class RecoveryFailedException extends Exception {

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public RecoveryFailedException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     * @param cause The cause of the failure.
     */
    public RecoveryFailedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
