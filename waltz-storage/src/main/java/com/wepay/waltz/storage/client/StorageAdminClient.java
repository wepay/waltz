package com.wepay.waltz.storage.client;

import com.wepay.riff.network.Message;
import com.wepay.riff.network.MessageCodec;
import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.MessageHandlerCallbacks;
import com.wepay.waltz.storage.common.message.SequenceMessage;
import com.wepay.waltz.storage.common.message.admin.AdminFailureResponse;
import com.wepay.waltz.storage.common.message.admin.AdminMessage;
import com.wepay.waltz.storage.common.message.admin.AdminMessageCodecV0;
import com.wepay.waltz.storage.common.message.admin.AdminMessageType;
import com.wepay.waltz.storage.common.message.admin.AdminOpenRequest;
import com.wepay.waltz.storage.common.message.admin.LastSessionInfoRequest;
import com.wepay.waltz.storage.common.message.admin.LastSessionInfoResponse;
import com.wepay.waltz.storage.common.message.admin.MetricsRequest;
import com.wepay.waltz.storage.common.message.admin.MetricsResponse;
import com.wepay.waltz.storage.common.message.admin.PartitionAssignmentRequest;
import com.wepay.waltz.storage.common.message.admin.PartitionAvailableRequest;
import com.wepay.waltz.storage.common.message.admin.RecordListRequest;
import com.wepay.waltz.storage.common.message.admin.RecordListResponse;
import com.wepay.waltz.storage.exception.StorageRpcException;
import io.netty.handler.ssl.SslContext;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class StorageAdminClient extends StorageBaseClient {
    private static final HashMap<Short, MessageCodec> CODECS = new HashMap<>();

    static {
        CODECS.put((short) 0, AdminMessageCodecV0.INSTANCE);
    }

    private static final String HELLO_MESSAGE = "Waltz Storage Admin Client";

    public StorageAdminClient(String host, int port, SslContext sslCtx, UUID key, int numPartitions) throws StorageRpcException {
        super(host, port, sslCtx, key, numPartitions);
    }

    @Override
    SequenceMessage getOpenRequest() {
        return new AdminOpenRequest(key, numPartitions);
    }

    /**
     * Sets a partition's available flag.
     * @param partitionId the partition id
     * @param isAvailable whether storage clients are allowed to read and write the partition
     * @return Future of Boolean
     */
    public CompletableFuture<Object> setPartitionAvailable(int partitionId, boolean isAvailable) {
        return call(new PartitionAvailableRequest(seqNum.getAndIncrement(), partitionId, isAvailable));
    }

    /**
     * Assign/unassign ownership of a partition for a storage client
     * @param partitionId the partition id
     * @param isAssigned whether the storage node has ownership of the partition
     * @param deleteStorageFiles whether to delete the storage files within the partition
     * @return Future of Boolean
     */
    public CompletableFuture<Object> setPartitionAssignment(int partitionId, boolean isAssigned, boolean deleteStorageFiles) {
        return call(new PartitionAssignmentRequest(seqNum.getAndIncrement(), partitionId, isAssigned, deleteStorageFiles));
    }

    /**
     * Gets metrics as a json string
     * @return Future of String
     */
    public CompletableFuture<Object> getMetrics() {
        return call(new MetricsRequest(seqNum.getAndIncrement()));
    }

    /**
     * Gets a list of transaction records starting from the specified transaction id
     * @param partitionId the partition id
     * @param transactionId the transaction id to start fetching
     * @param maxNumRecords the maximum number of records to fetch
     * @return Future of ArrayList of Records
     */
    public CompletableFuture<Object> getRecordList(int partitionId, long transactionId, int maxNumRecords) {
        return call(new RecordListRequest(seqNum.getAndIncrement(), partitionId, transactionId, maxNumRecords));
    }

    /**
     * Gets the session info of the last session
     * @param partitionId the partition id
     * @return Future of SessionInfo
     */
    public CompletableFuture<Object> lastSessionInfo(int partitionId) {
        return call(new LastSessionInfoRequest(seqNum.getAndIncrement(), partitionId));
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
            CompletableFuture<Object> future = pendingRequests.remove(((AdminMessage) msg).seqNum);

            if (future == null) {
                throw new IllegalArgumentException("receiver not found: messageType=" + msg.type());
            }

            switch (msg.type()) {
                case AdminMessageType.SUCCESS_RESPONSE:
                    future.complete(Boolean.TRUE);
                    break;

                case AdminMessageType.FAILURE_RESPONSE:
                    future.completeExceptionally(((AdminFailureResponse) msg).exception);
                    if (future == openStorageFuture) {
                        // A failure of an open request is fatal.
                        close();
                    }
                    break;

                case AdminMessageType.RECORD_LIST_RESPONSE:
                    future.complete(((RecordListResponse) msg).records);
                    break;

                case AdminMessageType.METRICS_RESPONSE:
                    future.complete(((MetricsResponse) msg).metricsJson);
                    break;

                case AdminMessageType.LAST_SESSION_INFO_RESPONSE:
                    future.complete(((LastSessionInfoResponse) msg).lastSessionInfo);
                    break;

                default:
                    throw new IllegalArgumentException("message not handled: messageType=" + msg.type());
            }
        }
    }

}
