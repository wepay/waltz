package com.wepay.waltz.storage.common.message.admin;

import com.wepay.waltz.storage.common.message.SequenceMessage;

public abstract class AdminMessage extends SequenceMessage {

    AdminMessage(long seqNum) {
        super(seqNum);
    }
}
