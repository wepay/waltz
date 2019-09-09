package com.wepay.waltz.common.message;

public class MountRequest extends FeedRequest {

    public final long seqNum;

    public MountRequest(ReqId reqId, long clientHighWaterMark, long seqNum) {
        super(reqId, clientHighWaterMark);

        this.seqNum = seqNum;
    }

    @Override
    public byte type() {
        return MessageType.MOUNT_REQUEST;
    }

}
