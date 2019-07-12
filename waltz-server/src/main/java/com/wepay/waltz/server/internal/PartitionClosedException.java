package com.wepay.waltz.server.internal;

public class PartitionClosedException extends Exception {

    public PartitionClosedException(String msg) {
        super(msg);
    }

}
