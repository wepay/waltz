package com.wepay.waltz.client.internal.network;

import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.network.NetworkClient;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.internal.Partition;
import com.wepay.waltz.common.message.CheckStorageConnectivityRequest;
import com.wepay.waltz.common.message.FeedRequest;
import com.wepay.waltz.common.message.HighWaterMarkRequest;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.MountRequest;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.message.TransactionDataRequest;
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
import java.util.concurrent.atomic.AtomicReference;

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
    private AtomicReference<CompletableFuture<Optional<Map<String, Boolean>>>> checkConnectivityRef;

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
        this.checkConnectivityRef = new AtomicReference<>();
    }

    /**
     * Shuts down this instance by
     *  - un-mounting all partitions,
     *  - setting {@link #running} to {@code false} and
     *  - completing the {@link #checkConnectivityRef}.future (with empty value) if its not already complete. This
     *  might be called if the server is down or if there is any network error.
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

                CompletableFuture<Optional<Map<String, Boolean>>> future = checkConnectivityRef.get();
                if (future != null) {
                    if (checkConnectivityRef.compareAndSet(future, null)) {
                        future.complete(Optional.empty());
                    }
                }
            }
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
     *
     * Note: There is a possibility that the channel may not be up when this method gets triggered. In that case,
     * CHECK_STORAGE_CONNECTIVITY_REQUEST is resent once the channel is active.
     *
     * @return Completable future with the connectivity status to the storage nodes within the cluster.
     */
    public CompletableFuture<Optional<Map<String, Boolean>>> checkServerToStorageConnectivity() {
        while (true) {
            CompletableFuture<Optional<Map<String, Boolean>>> future = checkConnectivityRef.get();
            if (future != null) {
                return future;
            } else {
                future = new CompletableFuture<>();
                if (checkConnectivityRef.compareAndSet(null, future)) {
                    sendCheckStorageConnectivityRequest();
                    return future;
                }
            }
        }
    }

    @Override
    protected MessageHandler getMessageHandler() {
        WaltzClientHandlerCallbacks handlerCallbacks = new WaltzClientHandlerCallbacksImpl();
        return new WaltzClientHandler(handlerCallbacks, messageProcessingThreadPool);
    }

    private void sendCheckStorageConnectivityRequest() {
        ReqId dummyReqId = new ReqId(clientId, 0, 0, 0);
        sendMessage(new CheckStorageConnectivityRequest(dummyReqId));
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

                // re-send the check storage connectivity request if waiting for the response.
                if (checkConnectivityRef.get() != null) {
                    sendCheckStorageConnectivityRequest();
                }
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

        /**
         * Handles the check connectivity response message received.
         * @param storageConnectivityMap Connectivity status map of all storage nodes within the cluster.
         */
        @Override
        public void onCheckStorageConnectivityResponseReceived(Map<String, Boolean> storageConnectivityMap) {
            CompletableFuture<Optional<Map<String, Boolean>>> future = checkConnectivityRef.get();
            if (future != null) {
                if (checkConnectivityRef.compareAndSet(future, null)) {
                    future.complete(Optional.of(storageConnectivityMap));
                }
            }
        }
    }
}
