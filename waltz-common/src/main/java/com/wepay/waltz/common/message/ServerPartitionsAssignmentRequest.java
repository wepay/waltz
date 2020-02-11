package com.wepay.waltz.common.message;

public class ServerPartitionsAssignmentRequest extends AbstractMessage {

    public ServerPartitionsAssignmentRequest(ReqId reqId) {
        super(reqId);
    }

    @Override
    public byte type() {
        return MessageType.SERVER_PARTITIONS_ASSIGNMENT_REQUEST;
    }
}
