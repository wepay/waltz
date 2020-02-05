package com.wepay.waltz.common.message;

import java.util.List;

public class ServerPartitionsAssignmentResponse extends AbstractMessage {

    public final List<Integer> serverPartitionAssignments;

    public ServerPartitionsAssignmentResponse(ReqId reqId, List<Integer> serverPartitionAssignments) {
        super(reqId);
        this.serverPartitionAssignments = serverPartitionAssignments;
    }

    @Override
    public byte type() {
        return MessageType.SERVER_PARTITIONS_ASSIGNMENT_RESPONSE;
    }
}
