package com.wepay.waltz.storage.exception;

public class StorageException extends Exception {

    public StorageException(String msg) {
        super(msg);
    }

    public StorageException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
