package com.wepay.waltz.store.exception;

public class RecoveryAbortedException extends RecoveryFailedException {

    public RecoveryAbortedException(Throwable cause) {
        super("recovery aborted", cause);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
