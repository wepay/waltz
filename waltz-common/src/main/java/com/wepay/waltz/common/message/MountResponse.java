package com.wepay.waltz.common.message;

public class MountResponse extends FeedSuspended {

    public static final class PartitionState {
        public static final int READY = 0;
        public static final int NOT_READY = 1;
        public static final int CLIENT_AHEAD = 2;

        private PartitionState() {
        }
    }

    public final int partitionState;

    public MountResponse(ReqId reqId, int partitionState) {
        super(reqId);
        this.partitionState = partitionState;
    }

    @Override
    public byte type() {
        return MessageType.MOUNT_RESPONSE;
    }

}
