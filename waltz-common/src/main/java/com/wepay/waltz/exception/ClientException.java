package com.wepay.waltz.exception;

public abstract class ClientException extends RuntimeException {

    public ClientException() {
        super();
    }

    public ClientException(String msg) {
        super(msg);
    }

}
