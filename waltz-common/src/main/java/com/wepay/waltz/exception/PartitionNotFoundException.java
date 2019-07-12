package com.wepay.waltz.exception;

public class PartitionNotFoundException extends ClientException {

    public PartitionNotFoundException(int partitionId) {
        super("partition not found: partitionId=" + partitionId);
    }

}
