package com.wepay.waltz.common.message;

import com.wepay.waltz.exception.RpcException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TransactionDataResponse extends AbstractMessage {

    public final long transactionId;
    public final byte[] data;
    public final int checksum;
    public final RpcException exception;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "internal class")
    public TransactionDataResponse(ReqId reqId, long transactionId, byte[] data, int checksum) {
        super(reqId);

        if (data == null) {
            throw new NullPointerException();
        }

        this.transactionId = transactionId;
        this.data = data;
        this.checksum = checksum;
        this.exception = null;
    }

    public TransactionDataResponse(ReqId reqId, long transactionId, RpcException exception) {
        super(reqId);

        if (exception == null) {
            throw new NullPointerException();
        }

        this.transactionId = transactionId;
        this.data = null;
        this.checksum = 0;
        this.exception = exception;
    }

    public byte type() {
        return MessageType.TRANSACTION_DATA_RESPONSE;
    }

}
