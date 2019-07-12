package com.wepay.waltz.common.message;

public class MountResponse extends FeedSuspended {

    public final boolean partitionReady;

    public MountResponse(ReqId reqId, boolean partitionReady) {
        super(reqId);
        this.partitionReady = partitionReady;
    }

    @Override
    public byte type() {
        return MessageType.MOUNT_RESPONSE;
    }

}
