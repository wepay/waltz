package com.wepay.waltz.common.message;

public class AddPreferredPartitionRequest extends AbstractMessage {

    public final int partitionId;
    public AddPreferredPartitionRequest(ReqId reqId, int partitionId) {
        super(reqId);
        this.partitionId = partitionId;
    }

    @Override
    public byte type() {
        return MessageType.ADD_PREFERRED_PARTITION_REQUEST;
    }
}
