package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if recovery is aborted.
 */
public class RecoveryAbortedException extends RecoveryFailedException {

    /**
     * Class constructor.
     * @param cause The cause of the failure.
     */
    public RecoveryAbortedException(Throwable cause) {
        super("recovery aborted", cause);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
