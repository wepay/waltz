package com.wepay.waltz.common.message;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class AppendRequest extends AbstractMessage {

    public final long clientHighWaterMark;
    public final int[] writeLockRequest;
    public final int[] readLockRequest;
    public final int[] appendLockRequest;
    public final int header;
    public final byte[] data;
    public final int checksum;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "internal class")
    public AppendRequest(
        ReqId reqId,
        long clientHighWaterMark,
        int[] writeLockRequest,
        int[] readLockRequest,
        int[] appendLockRequest,
        int header,
        byte[] data,
        int checksum
    ) {
        super(reqId);

        this.clientHighWaterMark = clientHighWaterMark;
        this.writeLockRequest = writeLockRequest;
        this.readLockRequest = readLockRequest;
        this.appendLockRequest = appendLockRequest;
        this.header = header;
        this.data = data;
        this.checksum = checksum;
    }

    @Override
    public byte type() {
        return MessageType.APPEND_REQUEST;
    }

}
