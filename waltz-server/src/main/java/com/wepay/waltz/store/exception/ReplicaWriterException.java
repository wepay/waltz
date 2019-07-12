package com.wepay.waltz.store.exception;

public class ReplicaWriterException extends ReplicaSessionException {

    public ReplicaWriterException(String msg) {
        super(msg);
    }

    public ReplicaWriterException(Throwable ex) {
        super(ex);
    }

    public ReplicaWriterException(String msg, Throwable ex) {
        super(msg, ex);
    }

}
