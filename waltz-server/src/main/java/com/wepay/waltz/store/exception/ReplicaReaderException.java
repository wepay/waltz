package com.wepay.waltz.store.exception;

public class ReplicaReaderException extends ReplicaSessionException {

    public ReplicaReaderException(String msg) {
        super(msg);
    }

    public ReplicaReaderException(Throwable ex) {
        super(ex);
    }

    public ReplicaReaderException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
