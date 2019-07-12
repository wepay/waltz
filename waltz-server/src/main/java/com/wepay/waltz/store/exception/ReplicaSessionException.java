package com.wepay.waltz.store.exception;

public class ReplicaSessionException extends Exception {

    public ReplicaSessionException(String msg) {
        super(msg);
    }

    public ReplicaSessionException(Throwable ex) {
        super(ex);
    }

    public ReplicaSessionException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
