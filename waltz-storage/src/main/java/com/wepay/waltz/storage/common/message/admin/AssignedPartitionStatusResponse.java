package com.wepay.waltz.storage.common.message.admin;

import java.util.Map;

public class AssignedPartitionStatusResponse extends AdminMessage {

    public final Map<Integer, Boolean> partitionStatusMap;
    public AssignedPartitionStatusResponse(long seqNum, Map<Integer, Boolean> partitionStatusMap) {
        super(seqNum);
        this.partitionStatusMap = partitionStatusMap;
    }

    @Override
    public byte type() {
        return AdminMessageType.ASSIGNED_PARTITION_STATUS_RESPONSE;
    }
}
