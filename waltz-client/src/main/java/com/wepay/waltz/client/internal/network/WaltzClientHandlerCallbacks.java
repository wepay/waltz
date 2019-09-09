package com.wepay.waltz.client.internal.network;

import com.wepay.riff.network.MessageHandlerCallbacks;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.ReqId;

/**
 * The interface for WaltzClientHandler callback methods, extends {@link MessageHandlerCallbacks}.
 */
public interface WaltzClientHandlerCallbacks extends MessageHandlerCallbacks {

    /**
     * Invoked if a partition is not yet ready to be mounted.
     *
     * @param partitionId the id of the partition.
     */
    void onPartitionNotReady(int partitionId);

    /**
     * Invoked when a partition is mounted.
     *
     * @param partitionId the id of the partition that was mounted.
     * @param sessionId the {@code ReqId} of the mount request.
     */
    void onPartitionMounted(int partitionId, ReqId sessionId);

    /**
     * Invoked if a waltz server notifies the client that, for partition {@code partitionId}, the feed catchup,
     * triggered by an earlier {@link com.wepay.waltz.common.message.FeedRequest} from the client, is suspended.
     *
     * The client should send a new FeedRequest to continue.
     *
     * @param partitionId the id of the partition
     * @param sessionId the {@code ReqId} of the original feed request.
     */
    void onFeedSuspended(int partitionId, ReqId sessionId);

    /**
     * Invoked when a transaction committed/appended response (a.k.a Feed data) is received.
     *
     * @param transactionId the id of the transaction.
     * @param header the header information of the transaction.
     * @param reqId the {@code ReqId} of the corresponding append request.
     */
    void onTransactionIdReceived(long transactionId, int header, ReqId reqId);

    /**
     * Invoked when the actual transaction data is received.
     *
     * @param partitionId the id of the partition this transaction was appended to.
     * @param transactionId the id of the transaction.
     * @param data the serialized data, an array of bytes, representing the transaction payload.
     * @param checksum the checksum information.
     * @param exception a {@code Throwable} this transaction is associated with, if any.
     */
    void onTransactionDataReceived(int partitionId, long transactionId, byte[] data, int checksum, Throwable exception);

    /**
     * Invoked when a flush is completed, i.e., when all pending transactions are processed.
     *
     * @param reqId the {@code ReqId} of the flush request.
     * @param transactionId the high-water mark after pending transactions are processed.
     */
    void onFlushCompleted(ReqId reqId, long transactionId);

    /**
     * Invoked if a lock request was failed.
     *
     * @param lockFailure a {@link LockFailure} object with the id of the transaction that made the lock request to fail.
     */
    void onLockFailed(LockFailure lockFailure);

}
