package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.ReqId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

public class StoreAppendRequest {

    public final ReqId reqId;
    public final int header;
    public final byte[] data;
    public final int checksum;

    private final LongConsumer callback;
    private final AtomicBoolean completed = new AtomicBoolean(false);

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "internal class")
    public StoreAppendRequest(ReqId reqId, int header, byte[] data, int checksum, LongConsumer callback) {
        this.reqId = reqId;
        this.header = header;
        this.data = data;
        this.checksum = checksum;
        this.callback = callback;
    }

    public void commit(long transactionId) {
        if (completed.compareAndSet(false, true)) {
            callback.accept(transactionId);

        } else {
            throw new IllegalStateException("request already committed: transactionId=" + transactionId);
        }
    }

    public boolean isCommitted() {
        return completed.get();
     }

}
