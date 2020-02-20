package com.wepay.waltz.common.message;

public class AddPreferredPartitionResponse extends AbstractMessage {

    public final Boolean result;
    public AddPreferredPartitionResponse(ReqId reqId, boolean result) {
        super(reqId);
        this.result = result;
    }

    @Override
    public byte type() {
        return MessageType.ADD_PREFERRED_PARTITION_RESPONSE;
    }
}
