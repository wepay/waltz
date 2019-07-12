package com.wepay.waltz.client.internal.network;

import com.wepay.riff.network.MessageHandlerCallbacks;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.ReqId;

public interface WaltzClientHandlerCallbacks extends MessageHandlerCallbacks {

    void onPartitionNotReady(int partitionId);

    void onPartitionMounted(int partitionId, ReqId sessionId);

    void onFeedSuspended(int partitionId, ReqId sessionId);

    void onTransactionIdReceived(long transactionId, int header, ReqId reqId);

    void onTransactionDataReceived(int partitionId, long transactionId, byte[] data, int checksum, Throwable exception);

    void onFlushCompleted(ReqId reqId, long transactionId);

    void onLockFailed(LockFailure lockFailure);

}
