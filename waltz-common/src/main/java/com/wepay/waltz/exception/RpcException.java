package com.wepay.waltz.exception;

public class RpcException extends ServerException {

    public RpcException(String msg) {
        super(msg);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
