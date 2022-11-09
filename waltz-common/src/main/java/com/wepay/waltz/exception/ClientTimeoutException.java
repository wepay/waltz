package com.wepay.waltz.exception;

public class ClientTimeoutException extends ClientException {
    public ClientTimeoutException(String message) {
        super(message);
    }
}
