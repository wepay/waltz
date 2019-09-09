package com.wepay.waltz.storage.exception;

public class ConcurrentUpdateException extends Exception {

    public ConcurrentUpdateException(String msg) {
        super(msg);
    }

}
