package com.wepay.waltz.storage.common.message;

import com.wepay.riff.network.Message;

public abstract class SequenceMessage extends Message {

    public final long seqNum;

    protected SequenceMessage(long seqNum) {
        this.seqNum = seqNum;
    }

}

