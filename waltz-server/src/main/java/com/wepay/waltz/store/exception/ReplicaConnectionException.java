package com.wepay.waltz.store.exception;

public class ReplicaConnectionException extends ReplicaSessionException {

    public ReplicaConnectionException(String msg) {
        super(msg);
    }

    public ReplicaConnectionException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
