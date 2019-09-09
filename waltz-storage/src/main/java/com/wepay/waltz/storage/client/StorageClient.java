package com.wepay.waltz.storage.client;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageCodec;
import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.MessageHandlerCallbacks;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.storage.common.message.AppendRequest;
import com.wepay.waltz.storage.common.message.FailureResponse;
import com.wepay.waltz.storage.common.message.LastSessionInfoRequest;
import com.wepay.waltz.storage.common.message.LastSessionInfoResponse;
import com.wepay.waltz.storage.common.message.MaxTransactionIdRequest;
import com.wepay.waltz.storage.common.message.MaxTransactionIdResponse;
import com.wepay.waltz.storage.common.message.OpenRequest;
import com.wepay.waltz.storage.common.message.RecordHeaderListRequest;
import com.wepay.waltz.storage.common.message.RecordHeaderListResponse;
import com.wepay.waltz.storage.common.message.RecordHeaderRequest;
import com.wepay.waltz.storage.common.message.RecordHeaderResponse;
import com.wepay.waltz.storage.common.message.RecordListRequest;
import com.wepay.waltz.storage.common.message.RecordListResponse;
import com.wepay.waltz.storage.common.message.RecordRequest;
import com.wepay.waltz.storage.common.message.RecordResponse;
import com.wepay.waltz.storage.common.message.SequenceMessage;
import com.wepay.waltz.storage.common.message.SetLowWaterMarkRequest;
import com.wepay.waltz.storage.common.message.StorageMessage;
import com.wepay.waltz.storage.common.message.StorageMessageCodecV0;
import com.wepay.waltz.storage.common.message.StorageMessageType;
import com.wepay.waltz.storage.common.message.TruncateRequest;
import io.netty.handler.ssl.SslContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class StorageClient extends StorageBaseClient {
    private static final HashMap<Short, MessageCodec> CODECS = new HashMap<>();

    private static final String HELLO_MESSAGE = "Waltz Storage Client";

    private final boolean usedByOfflineRecovery;

    static {
        CODECS.put((short) 0, StorageMessageCodecV0.INSTANCE);
    }

    public StorageClient(String host, int port, SslContext sslCtx, UUID key, int numPartitions) {
        this(host, port, sslCtx, key, numPartitions, false);
    }

    public StorageClient(String host, int port, SslContext sslCtx, UUID key, int numPartitions, boolean usedByOfflineRecovery) {
        super(host, port, sslCtx, key, numPartitions);
        this.usedByOfflineRecovery = usedByOfflineRecovery;
    }

    @Override
    SequenceMessage getOpenRequest() {
        return new OpenRequest(key, numPartitions);
    }

    /**
     * Gets the session info of the last session
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @return Future of SessionInfo
     */
    public CompletableFuture<Object> lastSessionInfo(long sessionId, int partitionId) {
        return call(new LastSessionInfoRequest(sessionId, seqNum.getAndIncrement(), partitionId, usedByOfflineRecovery));
    }

    /**
     * Sets the low-water mark of the current session
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @param lowWaterMark the low-water mark
     * @return Future of Boolean
     */
    public CompletableFuture<Object> setLowWaterMark(long sessionId, int partitionId, long lowWaterMark) {
        return call(new SetLowWaterMarkRequest(sessionId, seqNum.getAndIncrement(), partitionId, lowWaterMark, usedByOfflineRecovery));
    }

    /**
     * Truncates the transactions after the given transaction id.
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @param transactionId the max transaction id to be kept
     * @return Future of Boolean
     */
    public CompletableFuture<Object> truncate(long sessionId, int partitionId, long transactionId) {
        return call(new TruncateRequest(sessionId, seqNum.getAndIncrement(), partitionId, transactionId, usedByOfflineRecovery));
    }

    /**
     * Gets the transaction record header of the specified transaction id
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @param transactionId the transaction id
     * @return Future of RecordHeader
     */
    public CompletableFuture<Object> getRecordHeader(long sessionId, int partitionId, long transactionId) {
        return call(new RecordHeaderRequest(sessionId, seqNum.getAndIncrement(), partitionId, transactionId));
    }

    /**
     * Gets a list of transaction record headers starting from the specified transaction id
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @param transactionId the transaction id to start fetching
     * @param maxNumRecords the maximum number of records to fetch
     * @return Future of ArrayList of RecordHeaders
     */
    public CompletableFuture<Object> getRecordHeaderList(long sessionId, int partitionId, long transactionId, int maxNumRecords) {
        return call(new RecordHeaderListRequest(sessionId, seqNum.getAndIncrement(), partitionId, transactionId, maxNumRecords));
    }

    /**
     * Gets the transaction record of the specified transaction id
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @param transactionId the transaction id
     * @return Future of Record
     */
    public CompletableFuture<Object> getRecord(long sessionId, int partitionId, long transactionId) {
        return call(new RecordRequest(sessionId, seqNum.getAndIncrement(), partitionId, transactionId));
    }

    /**
     * Gets a list of transaction records starting from the specified transaction id
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @param transactionId the transaction id to start fetching
     * @param maxNumRecords the maximum number of records to fetch
     * @return Future of ArrayList of Records
     */
    public CompletableFuture<Object> getRecordList(long sessionId, int partitionId, long transactionId, int maxNumRecords) {
        return call(new RecordListRequest(sessionId, seqNum.getAndIncrement(), partitionId, transactionId, maxNumRecords));
    }

    /**
     * Gets the max transaction id in the storage
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @return Future of Long
     */
    public CompletableFuture<Object> getMaxTransactionId(long sessionId, int partitionId) {
        return call(new MaxTransactionIdRequest(sessionId, seqNum.getAndIncrement(), partitionId, usedByOfflineRecovery));
    }

    /**
     * Appends transaction records
     * @param sessionId the current session id
     * @param partitionId the partition id
     * @param records the records to append
     * @return Future of Boolean
     */
    public CompletableFuture<Object> appendRecords(long sessionId, int partitionId, ArrayList<Record> records) {
        return call(new AppendRequest(sessionId, seqNum.getAndIncrement(), partitionId, records, usedByOfflineRecovery));
    }

    @Override
    protected MessageHandler getMessageHandler() {
        return new MessageHandlerImpl(new StorageBaseClient.MessageHandlerCallbacksImpl());
    }

    private class MessageHandlerImpl extends MessageHandler {

        MessageHandlerImpl(MessageHandlerCallbacks callbacks) {
            super(CODECS, HELLO_MESSAGE, callbacks, 30, 60);
        }

        @Override
        protected void process(Message msg) throws Exception {
            CompletableFuture<Object> future = pendingRequests.remove(((StorageMessage) msg).seqNum);

            if (future == null) {
                throw new IllegalArgumentException("receiver not found: messageType=" + msg.type());
            }

            switch (msg.type()) {
                case StorageMessageType.LAST_SESSION_INFO_RESPONSE:
                    future.complete(((LastSessionInfoResponse) msg).lastSessionInfo);
                    break;

                case StorageMessageType.SUCCESS_RESPONSE:
                    future.complete(Boolean.TRUE);
                    break;

                case StorageMessageType.FAILURE_RESPONSE:
                    future.completeExceptionally(((FailureResponse) msg).exception);
                    if (future == openStorageFuture) {
                        // A failure of an open request is fatal.
                        close();
                    }
                    break;

                case StorageMessageType.RECORD_HEADER_RESPONSE:
                    future.complete(((RecordHeaderResponse) msg).recordHeader);
                    break;

                case StorageMessageType.RECORD_RESPONSE:
                    future.complete(((RecordResponse) msg).record);
                    break;

                case StorageMessageType.MAX_TRANSACTION_ID_RESPONSE:
                    future.complete(((MaxTransactionIdResponse) msg).transactionId);
                    break;

                case StorageMessageType.RECORD_HEADER_LIST_RESPONSE:
                    future.complete(((RecordHeaderListResponse) msg).recordHeaders);
                    break;

                case StorageMessageType.RECORD_LIST_RESPONSE:
                    future.complete(((RecordListResponse) msg).records);
                    break;

                default:
                    throw new IllegalArgumentException("message not handled: messageType=" + msg.type());
            }
        }
    }

}
