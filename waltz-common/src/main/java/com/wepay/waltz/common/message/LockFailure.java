package com.wepay.waltz.common.message;

import java.util.Comparator;

public class LockFailure extends AbstractMessage {

    public static final Comparator<LockFailure> COMPARATOR = (a, b) -> Long.compare(a.transactionId, b.transactionId);

    public final long transactionId;

    public LockFailure(ReqId reqId, long transactionId) {
        super(reqId);
        this.transactionId = transactionId;
    }

    @Override
    public byte type() {
        return MessageType.LOCK_FAILURE;
    }

}
