package com.wepay.waltz.common.message;

public class RemovePreferredPartitionResponse extends AbstractMessage {

    public final Boolean result;
    public RemovePreferredPartitionResponse(ReqId reqId, boolean result) {
        super(reqId);
        this.result = result;
    }

    @Override
    public byte type() {
        return MessageType.REMOVE_PREFERRED_PARTITION_RESPONSE;
    }
}
