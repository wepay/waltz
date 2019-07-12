package com.wepay.waltz.store.exception;

public class RecoveryFailedException extends Exception {

    public RecoveryFailedException(String msg) {
        super(msg);
    }

    public RecoveryFailedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
