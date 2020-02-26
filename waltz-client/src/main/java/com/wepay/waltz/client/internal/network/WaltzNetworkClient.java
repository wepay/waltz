package com.wepay.waltz.client.internal.network;

import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.network.NetworkClient;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.internal.Partition;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.AddPreferredPartitionRequest;
import com.wepay.waltz.common.message.CheckStorageConnectivityRequest;
import com.wepay.waltz.common.message.RemovePreferredPartitionRequest;
import com.wepay.waltz.common.message.FeedRequest;
import com.wepay.waltz.common.message.HighWaterMarkRequest;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.MessageType;
import com.wepay.waltz.common.message.MountRequest;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.message.TransactionDataRequest;
import com.wepay.waltz.common.message.ServerPartitionsAssignmentRequest;
import com.wepay.waltz.exception.NetworkClientClosedException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.util.Uninterruptibly;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link NetworkClient} implementation for waltz clients to communicate with Waltz cluster.
 *
 * One {@link WaltzNetworkClient} instance is associated with exactly one server in the Waltz cluster.
 */
public class WaltzNetworkClient extends NetworkClient {

    private static final Logger logger = Logging.getLogger(WaltzNetworkClient.class);

    public final int clientId;
    public final Endpoint endpoint;
    public final long seqNum;

    private final WaltzNetworkClientCallbacks networkClientCallbacks;
    private final MessageProcessingThreadPool messageProcessingThreadPool;
    private final Object lock = new Object();
    private final HashMap<Integer, Partition> partitions;

    private boolean channelReady = false;
    private volatile boolean running = true;
    private Map<Integer, CompletableFuture<Object>> outputFuturesPerMessageType;

    /**
     * Class Constructor.
     *
     * @param clientId Unique id assigned to an instance of {@link com.wepay.waltz.client.WaltzClient} on creation.
     * @param endpoint {@link Endpoint} Endpoint of the physical server this instance will be responsible for.
     * @param sslCtx {@link SslContext} SSL context required for communication
     * @param seqNum Sequence number of the {@link WaltzNetworkClient} responsible for the server.
     * @param networkClientCallbacks  {@link WaltzNetworkClientCallbacks}
     * @param messageProcessingThreadPool {@link MessageProcessingThreadPool}
     */
    public WaltzNetworkClient(
        int clientId,
        Endpoint endpoint,
        SslContext sslCtx,
        long seqNum,
        WaltzNetworkClientCallbacks networkClientCallbacks,
        MessageProcessingThreadPool messageProcessingThreadPool
    ) {
        super(endpoint.host, endpoint.port, sslCtx);

        this.clientId = clientId;
        this.endpoint = endpoint;
        this.seqNum = seqNum;
        this.networkClientCallbacks = networkClientCallbacks;
        this.messageProcessingThreadPool = messageProcessingThreadPool;
        this.partitions = new HashMap<>();
        this.outputFuturesPerMessageType = new ConcurrentHashMap<>();
    }

    /**
     * Shuts down this instance by
     *  - Un-mounting all partitions,
     *  - Setting {@link #running} to {@code false}, and
     *  - Completing the futures in {@code outputFuturesPerMessageType} with an exception
     */
    @Override
    protected void shutdown() {
        super.shutdown();

        synchronized (lock) {
            if (running) {
                running = false;

                for (Partition partition : partitions.values()) {
                    partition.unmounted(this);
                }

                outputFuturesPerMessageType.values()
                    .stream()
                    .filter(future -> !future.isDone())
                    .forEach(future -> future.completeExceptionally(new NetworkClientClosedException()));
            }

            lock.notifyAll();
        }
        networkClientCallbacks.onNetworkClientDisconnected(this);
    }

    /**
     * Mounts a {@link Partition} to the server owned by this instance,
     * and invokes {@link WaltzNetworkClientCallbacks#onMountingPartition(WaltzNetworkClient, Partition)}.
     *
     * @param partition {@code Partition} to mount.
     */
    public void mountPartition(Partition partition) {
        synchronized (lock) {
            partitions.put(partition.partitionId, partition); // always do this for recovery even when not running
            if (running) {
                // Tell the partition that the client is mounting through this network client
                partition.mounting(this);

                // Actually starting to mount partitions. Call onMountingPartition if the channel is ready
                // Otherwise, we do it when the channel becomes ready
                if (channelReady) {
                    networkClientCallbacks.onMountingPartition(this, partition);
                }
            }
        }
    }

    /**
     * Un-mounts a specific partition.
     *
     * @param partitionId id of the partition to un-mount.
     */
    public void unmountPartition(int partitionId) {
        synchronized (lock) {
            Partition partition = partitions.remove(partitionId);
            if (partition != null) {
                partition.unmounted(this);
            }
        }
    }

    /**
     * Un-mounts all partitions.
     *
     * @return list of all partitions to un-mount.
     */
    public List<Partition> unmountAllPartitions() {
        synchronized (lock) {
            ArrayList<Partition> list = new ArrayList<>(partitions.values());
            for (Partition partition : list) {
                unmountPartition(partition.partitionId);
            }
            return list;
        }
    }

    /**
     * Requests transaction data for a given transactionId.
     *
     * @param reqId reqId of the {@link TransactionDataRequest}.
     * @param transactionId id of the transaction.
     * @throws NetworkClientClosedException if this instance is already closed.
     */
    public void requestTransactionData(ReqId reqId, long transactionId) {
        synchronized (lock) {
            if (!running) {
                throw new NetworkClientClosedException();
            }

            if (channelReady) {
                sendMessage(new TransactionDataRequest(reqId, transactionId));
            } else {
                logger.info("failed to send transaction data request, channel not ready: partitionId=" + reqId.partitionId());
            }
        }
    }

    /**
     * Requests high watermark for a given transactionId.
     *
     * @param reqId reqId of the {@link HighWaterMarkRequest}.
     * @throws NetworkClientClosedException if this instance is already closed.
     */
    public void requestHighWaterMark(ReqId reqId) {
        synchronized (lock) {
            if (!running) {
                throw new NetworkClientClosedException();
            }

            if (channelReady) {
                sendMessage(new HighWaterMarkRequest(reqId));
            } else {
                logger.info("failed to send high watermark request, channel not ready: partitionId=" + reqId.partitionId());
            }
        }
    }

    /**
     * Checks server to storage connectivity.
     * The current thread waits for channel readiness before sending the request to the server. If this client
     * is shutdown before that, an exceptionally completed CompletableFuture is returned.
     *
     * @return a CompletableFuture which will complete with connectivity statuses between this server and
     * all the storage nodes in the cluster, or with an exception, if any.
     * @throws InterruptedException If thread interrupted while waiting for channel to be ready.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, Boolean>> checkServerToStorageConnectivity() throws InterruptedException {
        ReqId dummyReqId = new ReqId(clientId, 0, 0, 0);
        CompletableFuture<Object> responseFuture =
            sendRequestOnChannelActive(new CheckStorageConnectivityRequest(dummyReqId));

        return responseFuture.thenApply(response -> (Map<String, Boolean>) response);
    }

    /**
     * Requests the list of partitions assigned to this server.
     * The current thread waits for channel readiness before sending the request to the server. If this client
     * is shutdown before that, an exceptionally completed CompletableFuture is returned.
     *
     * @return a CompletableFuture which will complete with the list of partitions assigned to this server,
     * or with an exception, if any.
     * @throws InterruptedException If thread interrupted while waiting for the channel to be ready
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Integer>> getServerPartitionAssignments() throws InterruptedException {
        ReqId dummyReqId = new ReqId(clientId, 0, 0, 0);
        CompletableFuture<Object> responseFuture =
            sendRequestOnChannelActive(new ServerPartitionsAssignmentRequest(dummyReqId));

        return responseFuture.thenApply(response -> (List<Integer>) response);
    }

    /**
     * Adds the given partition Id as a preferred partition on the server.
     * The current thread waits for channel readiness before sending the request to the server. If this client
     * is shutdown before that, an exceptionally completed CompletableFuture is returned.
     *
     * @param partitionId The partition Id to be added.
     * @return a CompletableFuture which will complete with a {@code true} if the preferred partition is added
     * successfully, a {@code false} otherwise, or an exception if any.
     * @throws InterruptedException If thread interrupted while waiting for the channel to be ready.
     */
    public CompletableFuture<Boolean> addPreferredPartition(int partitionId) throws InterruptedException {
        ReqId dummyReqId = new ReqId(clientId, 0, partitionId, 0);
        CompletableFuture<Object> responseFuture =
            sendRequestOnChannelActive(new AddPreferredPartitionRequest(dummyReqId, partitionId));

        return responseFuture.thenApply(response -> (Boolean) response);
    }

    /**
     * Removes the given partition Id as a preferred partition on the server.
     * The current thread waits for channel readiness before sending the request to the server. If this client
     * is shutdown before that, an exceptionally completed CompletableFuture is returned.
     *
     * @param partitionId The partition Id to be removed.
     * @return a CompletableFuture which will complete with a {@code true} if the preferred partition is removed
     * successfully, a {@code false} otherwise, or an exception if any.
     * @throws InterruptedException If thread interrupted while waiting for the channel to be ready.
     */
    public CompletableFuture<Boolean> removePreferredPartition(int partitionId) throws InterruptedException {
        ReqId dummyReqId = new ReqId(clientId, 0, partitionId, 0);
        CompletableFuture<Object> responseFuture =
            sendRequestOnChannelActive(new RemovePreferredPartitionRequest(dummyReqId, partitionId));

        return responseFuture.thenApply(response -> (Boolean) response);
    }

    private CompletableFuture<Object> sendRequestOnChannelActive(AbstractMessage requestMessage) throws InterruptedException {
        synchronized (lock) {
            while (running && !channelReady) {
                lock.wait();
            }

            if (!running) {
                CompletableFuture<Object> future = new CompletableFuture<>();
                future.completeExceptionally(new NetworkClientClosedException());
                return future;
            }
        }

        return outputFuturesPerMessageType
                .compute(
                    (int) requestMessage.type(),
                    (keyMessageType, valueFuture) -> {
                        if (valueFuture != null && !(valueFuture.isDone())) {
                            return valueFuture;
                        }

                        CompletableFuture<Object> newFuture = new CompletableFuture<>();

                        if (!sendMessage(requestMessage)) {
                            newFuture.completeExceptionally(new Exception("Couldn't send message"));
                        }
                        return newFuture;
                    });
    }

    @Override
    protected MessageHandler getMessageHandler() {
        WaltzClientHandlerCallbacks handlerCallbacks = new WaltzClientHandlerCallbacksImpl();
        return new WaltzClientHandler(handlerCallbacks, messageProcessingThreadPool);
    }

    private Partition getPartition(int partitionId) {
        synchronized (lock) {
            return partitions.get(partitionId);
        }
    }

    private class WaltzClientHandlerCallbacksImpl implements WaltzClientHandlerCallbacks {
        @Override
        public void onChannelActive() {
            synchronized (lock) {
                channelReady = true;

                // Actually starting to mount partitions. Call onMountingPartition
                for (Partition partition : partitions.values()) {
                    networkClientCallbacks.onMountingPartition(WaltzNetworkClient.this, partition);
                }

                lock.notifyAll();
            }
        }

        @Override
        public void onChannelInactive() {
            synchronized (lock) {
                channelReady = false;

                if (running) {
                    // This happens when the server was shutdown or there was a network error
                    // Close the network client to guarantee the strong order semantics
                    closeAsync();
                }
            }
        }

        @Override
        public void onWritabilityChanged(boolean isWritable) {
            // Do nothing
        }

        @Override
        public void onExceptionCaught(Throwable ex) {
            // Do nothing
        }

        @Override
        public void onPartitionNotReady(int partitionId) {
            Partition partition = getPartition(partitionId);
            if (partition != null) {
                // Retry since this partition is still assigned to this server
                logger.info("Partition was not ready, retrying: partitionId=" + partitionId + " server=" + endpoint);
                // Backoff
                Uninterruptibly.sleep(500);
                sendMessage(new MountRequest(partition.nextReqId(), partition.clientHighWaterMark(), seqNum));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("partition not found: event=onPartitionNotReady partitionId=" + partitionId);
                }
            }
        }

        @Override
        public void onPartitionMounted(int partitionId, ReqId sessionId) {
            synchronized (lock) {
                Partition partition = partitions.get(partitionId);
                if (partition != null) {
                    partition.mounted(WaltzNetworkClient.this);

                    // Send a feed request
                    long clientHighWaterMark = partition.clientHighWaterMark();
                    sendMessage(new FeedRequest(sessionId, clientHighWaterMark));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("partition not found: event=onPartitionMounted partitionId=" + partitionId);
                    }
                }
            }
        }

        @Override
        public void onFeedSuspended(int partitionId, ReqId sessionId) {
            // Resume the feed immediately.
            Partition partition = getPartition(partitionId);
            if (partition != null) {
                long clientHighWaterMark = partition.clientHighWaterMark();
                sendMessage(new FeedRequest(sessionId, clientHighWaterMark));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("partition not found: event=onFeedSuspended partitionId=" + partitionId);
                }
            }
        }

        @Override
        public void onTransactionIdReceived(long transactionId, int header, ReqId reqId) {
            Partition partition = getPartition(reqId.partitionId());

            // Ignore the transaction if it has an unexpected partition
            if (partition != null) {
                partition.applyTransaction(transactionId, header, reqId, networkClientCallbacks);

            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("partition not found: event=onTransactionIdReceived partitionId=" + reqId.partitionId() + " transactionId=" + transactionId);
                }
            }
        }

        @Override
        public void onTransactionDataReceived(int partitionId, long transactionId, byte[] data, int checksum, Throwable exception) {
            Partition partition = getPartition(partitionId);

            if (partition != null) {
                partition.transactionDataReceived(transactionId, data, checksum, exception);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("partition not found: event=onTransactionDataReceived partitionId=" + partitionId + " transactionId=" + transactionId);
                }
            }
        }

        @Override
        public void onFlushCompleted(ReqId reqId, long transactionId) {
            Partition partition = getPartition(reqId.partitionId());

            if (partition != null) {
                partition.flushCompleted(reqId, transactionId);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("partition not found: event=onFlushCompleted partitionId=" + reqId.partitionId());
                }
            }
        }

        @Override
        public void onLockFailed(LockFailure lockFailure) {
            ReqId reqId = lockFailure.reqId;
            Partition partition = getPartition(reqId.partitionId());

            if (partition != null) {
                partition.lockFailed(lockFailure);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("partition not found: event=onFlushCompleted partitionId=" + reqId.partitionId());
                }
            }
        }

        @Override
        public void onHighWaterMarkReceived(int partitionId, long highWaterMark) {
            Partition partition = getPartition(partitionId);

            if (partition != null) {
                partition.highWaterMarkReceived(highWaterMark);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("partition not found: event=onHighWaterMarkReceived partitionId=" + partitionId);
                }
            }
        }

        @Override
        public void onCheckStorageConnectivityResponseReceived(Map<String, Boolean> storageConnectivityMap) {
            outputFuturesPerMessageType.computeIfPresent(
                MessageType.CHECK_STORAGE_CONNECTIVITY_REQUEST,
                (keyMessageType, valueFuture) -> {
                    if (!valueFuture.isDone()) {
                        valueFuture.complete(storageConnectivityMap);
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                }
            );
        }

        @Override
        public void onServerPartitionsAssignmentResponseReceived(List<Integer> partitions) {
            outputFuturesPerMessageType.computeIfPresent(
                MessageType.SERVER_PARTITIONS_ASSIGNMENT_REQUEST,
                (keyMessageType, valueFuture) -> {
                    if (!valueFuture.isDone()) {
                        valueFuture.complete(partitions);
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                }
            );
        }

        @Override
        public void onAddPreferredPartitionResponseReceived(Boolean result) {
            outputFuturesPerMessageType.computeIfPresent(
                MessageType.ADD_PREFERRED_PARTITION_REQUEST,
                (keyMessageType, valueFuture) -> {
                    if (!valueFuture.isDone()) {
                        valueFuture.complete(result);
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                }
            );
        }

        @Override
        public void onRemovePreferredPartitionResponseReceived(Boolean result) {
            outputFuturesPerMessageType.computeIfPresent(
                MessageType.REMOVE_PREFERRED_PARTITION_REQUEST,
                (keyMessageType, valueFuture) -> {
                    if (!valueFuture.isDone()) {
                        valueFuture.complete(result);
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                }
            );
        }
    }
}
