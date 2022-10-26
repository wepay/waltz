package com.wepay.waltz.exception;

public class WaitForMountedTimeoutException extends ClientException {
    public WaitForMountedTimeoutException(int partitionId, int timeout) {
        super(String.format("client couldn't ensure that partition: %d is mounted within %d ms", partitionId, timeout));
    }
}
