package com.wepay.waltz.storage.common.message.admin;

public class AssignedPartitionStatusRequest extends AdminMessage {

    public AssignedPartitionStatusRequest(long seqNum) {
        super(seqNum);
    }

    @Override
    public byte type() {
        return AdminMessageType.ASSIGNED_PARTITION_STATUS_REQUEST;
    }
}
