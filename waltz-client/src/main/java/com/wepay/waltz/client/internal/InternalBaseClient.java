package com.wepay.waltz.client.internal;

import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.internal.network.WaltzNetworkClient;
import com.wepay.waltz.client.internal.network.WaltzNetworkClientCallbacks;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.DaemonThreadFactory;
import com.wepay.waltz.exception.ClientClosedException;
import com.wepay.waltz.exception.InvalidOperationException;
import com.wepay.waltz.exception.PartitionNotFoundException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.PartitionInfo;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class InternalBaseClient implements WaltzNetworkClientCallbacks, ManagedClient {

    private static final Logger logger = Logging.getLogger(InternalBaseClient.class);

    private int clientId = -1;
    private String clusterName = null;
    protected int numPartitions = 0;

    private final boolean autoMount;
    private final SslContext sslCtx;
    private final int maxConcurrentTransactions;
    protected final WaltzClientCallbacks callbacks;
    private final MessageProcessingThreadPool messageProcessingThreadPool;
    private final HashMap<Endpoint, WaltzNetworkClient> networkClients = new HashMap<>();
    private final ConcurrentHashMap<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService asyncTaskExecutor = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.INSTANCE);
    private final Set<Integer> activePartitions = new HashSet<>();

    private volatile Map<Endpoint, List<PartitionInfo>> endpoints;
    private volatile boolean running = true;

    protected InternalBaseClient(
        boolean autoMount,
        SslContext sslCtx,
        int maxConcurrentTransactions,
        WaltzClientCallbacks callbacks,
        MessageProcessingThreadPool messageProcessingThreadPool
    ) {
        this.autoMount = autoMount;
        this.sslCtx = sslCtx;
        this.maxConcurrentTransactions = maxConcurrentTransactions;
        this.callbacks = callbacks;
        this.messageProcessingThreadPool = messageProcessingThreadPool;
        this.endpoints = Collections.emptyMap();
    }

    public void close() {
        synchronized (networkClients) {
            running = false;

            for (WaltzNetworkClient networkClient : networkClients.values()) {
                networkClient.close();
            }
            networkClients.clear();

            for (Partition partition : partitions.values()) {
                partition.close();
            }
            partitions.clear();

            asyncTaskExecutor.shutdownNow();
        }
    }

    public void setActivePartitions(Set<Integer> partitionIds) {
        if (autoMount) {
            throw new InvalidOperationException("failed to set partitions: the client is configured with auto-mount on");
        }

        synchronized (activePartitions) {
            for (Partition partition : partitions.values()) {
                if (partitionIds.contains(partition.partitionId)) {
                    activePartitions.add(partition.partitionId);

                } else {
                    activePartitions.remove(partition.partitionId);
                    partition.deactivate();

                    synchronized (networkClients) {
                        // Unmount the partition.
                        for (WaltzNetworkClient networkClient : networkClients.values()) {
                            networkClient.unmountPartition(partition.partitionId);
                        }
                    }
                }
            }
        }

        // Activate partitions
        doSetEndpoints();
    }

    public Set<Integer> getActivePartitions() {
        return new HashSet<>(activePartitions);
    }

    // WaltzNetworkClientCallbacks API
    @Override
    public abstract void onMountingPartition(WaltzNetworkClient networkClient, Partition partition);

    // WaltzNetworkClientCallbacks API
    @Override
    public void onNetworkClientDisconnected(final WaltzNetworkClient closedNetworkClient) {
        // If the network client in use is closed, it is a recoverable failure.
        // We will asynchronously recover from the connection loss

        long delay = closedNetworkClient.openFailed() ? 1000 : 0;

        submitAsyncTask(
            () -> {
                try {
                    synchronized (networkClients) {
                        // We try to recover only when running.
                        if (running) {
                            Endpoint endpoint = closedNetworkClient.endpoint;
                            long seqNum = closedNetworkClient.seqNum + 1;

                            // Make sure this network client is the one actually being used
                            if (networkClients.get(endpoint) == closedNetworkClient) {
                                logger.info("recovering from connection loss: clientId=" + clientId + " endpoint=" + endpoint);

                                // Create a new network client and move all partitions from the old network client to it.
                                WaltzNetworkClient nc = new WaltzNetworkClient(clientId, endpoint, sslCtx, seqNum, this, messageProcessingThreadPool);
                                networkClients.put(endpoint, nc);
                                nc.openAsync();

                                for (Partition partition : closedNetworkClient.unmountAllPartitions()) {
                                    nc.mountPartition(partition);
                                }

                                logger.info("recovered from connection loss: clientId=" + clientId + " endpoint=" + endpoint + " seqNum=" + nc.seqNum);
                            }
                        }
                    }
                } catch (Throwable ex) {
                    logger.error("failed to recover from connection loss", ex);
                }
            },
            delay
        );
    }

    // WaltzNetworkClientCallbacks API
    @Override
    public void onTransactionReceived(long transactionId, int header, ReqId reqId) {
        throw new UnsupportedOperationException("not supported by " + this.getClass().getSimpleName());
    }

    // ManagedClient API
    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    // ManagedClient API
    @Override
    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    // ManagedClient API
    @Override
    public void setNumPartitions(int numPartitions) {
        if (this.numPartitions != 0) {
            if (this.numPartitions == numPartitions) {
                return;
            }
            // TODO: online change
            throw new UnsupportedOperationException("the number of partitions cannot be changed");
        }

        this.numPartitions = numPartitions;

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            Partition partition = new Partition(partitionId, clientId, maxConcurrentTransactions);
            partitions.put(partitionId, partition);
        }

        if (autoMount) {
            synchronized (activePartitions) {
                activePartitions.addAll(partitions.keySet());

                for (Partition partition : partitions.values()) {
                    partition.activate(callbacks.getClientHighWaterMark(partition.partitionId));
                }
            }
        }
    }

    // ManagedClient API
    @Override
    public void setEndpoints(Map<Endpoint, List<PartitionInfo>> endpoints) {
        logger.debug("submitting setEndpoints task");

        // Save endpoints. No need to make a copy of map since ClusterManager always creates a new map before calling setEndpoints().
        this.endpoints = endpoints;

        submitAsyncTask(this::doSetEndpoints);
    }

    // ManagedClient API
    @Override
    public void removeServer(Endpoint endpoint) {
        logger.debug("submitting removeServer task: endpoint={}", endpoint);

        submitAsyncTask(() -> {
            synchronized (networkClients) {
                logger.info("removing server: endpoint=" + endpoint);
                WaltzNetworkClient networkClient = networkClients.remove(endpoint);
                if (networkClient != null) {
                    networkClient.close();
                }
            }
        });
    }

    public int clientId() {
        return clientId;
    }

    public String clusterName() {
        return clusterName;
    }

    public void flushTransactions() {
        ArrayList<TransactionFuture> futures = new ArrayList<>(partitions.size());
        for (Partition partition : partitions.values()) {
            TransactionFuture future = partition.flushTransactionsAsync();
            if (future != null) {
                futures.add(future);
            }
        }

        for (TransactionFuture future : futures) {
            // Wait for flush to complete
            future.awaitFlush();
            if (!running) {
                break;
            }
        }
    }

    public void nudgeWaitingTransactions(long longWaitThreshold) {
        for (Partition partition : partitions.values()) {
            partition.nudgeWaitingTransactions(longWaitThreshold);
        }
    }

    public boolean hasPendingTransactions() {
        for (Partition partition : partitions.values()) {
            if (partition.hasPendingTransactions()) {
                return true;
            }
        }
        return false;
    }

    protected Partition getPartition(int partitionId) {
        if (running) {
            Partition partition = partitions.get(partitionId);
            if (partition == null) {
                throw new PartitionNotFoundException(partitionId);
            }
            return partition;
        } else {
            throw new ClientClosedException();
        }
    }

    private WaltzNetworkClient getNetworkClient(Endpoint endpoint) {
        WaltzNetworkClient networkClient = networkClients.get(endpoint);
        if (networkClient == null) {
            networkClient = new WaltzNetworkClient(clientId, endpoint, sslCtx, 0, this, messageProcessingThreadPool);
            networkClients.put(endpoint, networkClient);
            networkClient.openAsync();
        }

        return networkClient;
    }

    private void submitAsyncTask(Runnable runnable) {
        submitAsyncTask(runnable, 0L);
    }

    private void submitAsyncTask(Runnable runnable, long delay) {
        try {
            if (running) {
                asyncTaskExecutor.schedule(runnable, delay, TimeUnit.MILLISECONDS);
            }

        } catch (RejectedExecutionException ex) {
            // If running, it is unexpected to get RejectedExecutionException. Otherwise, ignore.
            if (running) {
                throw ex;
            }
        }
    }

    private void doSetEndpoints() {
        try {
            synchronized (activePartitions) {
                for (Map.Entry<Endpoint, List<PartitionInfo>> entry : endpoints.entrySet()) {
                    Endpoint endpoint = entry.getKey();

                    for (PartitionInfo partitionInfo : entry.getValue()) {
                        Partition partition = getPartition(partitionInfo.partitionId);

                        // Update the generation number
                        partition.generation(partitionInfo.generation);

                        if (activePartitions.contains(partitionInfo.partitionId)) {
                            // This partition is supposed to be active. If not, activate it.
                            if (!partition.isActive()) {
                                partition.activate(callbacks.getClientHighWaterMark(partitionInfo.partitionId));
                            }

                            // Check if the partition is moved from another server.
                            if (!endpoint.eq(partition.endPoint())) {
                                // The partition is moved. Remount it now.
                                synchronized (networkClients) {
                                    for (WaltzNetworkClient networkClient : networkClients.values()) {
                                        networkClient.unmountPartition(partitionInfo.partitionId);
                                    }
                                    getNetworkClient(endpoint).mountPartition(partition);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Throwable ex) {
            logger.error("failed to set endpoints", ex);
        }
    }

}
