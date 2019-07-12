package com.wepay.waltz.common.message;

public class TransactionDataRequest extends AbstractMessage {

    public final long transactionId;

    public TransactionDataRequest(ReqId reqId, long transactionId) {
        super(reqId);

        this.transactionId = transactionId;
    }

    public byte type() {
        return MessageType.TRANSACTION_DATA_REQUEST;
    }

}
