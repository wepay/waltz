package com.wepay.waltz.exception;

public class PartitionInactiveException extends ClientException {

    public PartitionInactiveException(int partitionId) {
        super("partition inactive: partitionId=" + partitionId);
    }
}
