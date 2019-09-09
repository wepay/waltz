package com.wepay.waltz.storage.common.message.admin;

public class AdminSuccessResponse extends AdminMessage {

    public AdminSuccessResponse(long seqNum) {
        super(seqNum);
    }

    @Override
    public byte type() {
        return AdminMessageType.SUCCESS_RESPONSE;
    }

}
