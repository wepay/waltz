package com.wepay.waltz.common.message;

public class RemovePreferredPartitionRequest extends AbstractMessage {

    public final int partitionId;

    public RemovePreferredPartitionRequest(ReqId reqId, int partitionId) {
        super(reqId);
        this.partitionId = partitionId;
    }

    @Override
    public byte type() {
        return MessageType.REMOVE_PREFERRED_PARTITION_REQUEST;
    }
}
