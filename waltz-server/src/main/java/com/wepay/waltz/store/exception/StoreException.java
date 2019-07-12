package com.wepay.waltz.store.exception;

public class StoreException extends Exception {

    public StoreException() {
        super();
    }

    public StoreException(Throwable cause) {
        super(cause);
    }

    public StoreException(String msg) {
        super(msg);
    }

    public StoreException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
