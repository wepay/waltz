package com.wepay.waltz.common.message;

import com.wepay.riff.network.Message;

public abstract class AbstractMessage extends Message {

    public final ReqId reqId;

    protected AbstractMessage(ReqId reqId) {
        this.reqId = reqId;
    }

}
