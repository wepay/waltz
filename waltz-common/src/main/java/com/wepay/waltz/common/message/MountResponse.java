package com.wepay.waltz.common.message;

public class MountResponse extends FeedSuspended {

    public final boolean partitionReady;
    public final boolean partitionException;

    public MountResponse(ReqId reqId, boolean partitionReady, boolean partitionException) {
        super(reqId);
        this.partitionReady = partitionReady;
        this.partitionException = partitionException;
    }

    @Override
    public byte type() {
        return MessageType.MOUNT_RESPONSE;
    }

}
