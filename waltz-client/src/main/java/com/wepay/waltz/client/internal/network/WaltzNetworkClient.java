package com.wepay.waltz.client.internal.network;

import com.wepay.riff.network.MessageHandler;
import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.network.NetworkClient;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.internal.Partition;
import com.wepay.waltz.common.message.FeedRequest;
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
    }

    protected void shutdown() {
        super.shutdown();

        synchronized (lock) {
            if (running) {
                running = false;

                for (Partition partition : partitions.values()) {
                    partition.unmounted(this);
                }
            }
        }
        networkClientCallbacks.onNetworkClientDisconnected(this);
    }

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

    public void unmountPartition(int partitionId) {
        synchronized (lock) {
            Partition partition = partitions.remove(partitionId);
            if (partition != null) {
                partition.unmounted(this);
            }
        }
    }

    public List<Partition> unmountAllPartitions() {
        synchronized (lock) {
            ArrayList<Partition> list = new ArrayList<>(partitions.values());
            for (Partition partition : list) {
                unmountPartition(partition.partitionId);
            }
            return list;
        }
    }

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
    }
}
