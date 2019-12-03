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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An abstract internal client to communicate with waltz cluster over a network,
 * implements {@link WaltzNetworkClientCallbacks} and {@link ManagedClient}.
 */
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

    /**
     * Class Constructor.
     *
     * @param autoMount if {@code true}, automatically mount all partitions.
     * @param sslCtx SSLContext for communication.
     * @param maxConcurrentTransactions Max number of concurrent transactions allowed.
     * @param callbacks a {@code WaltzClientCallbacks} instance.
     * @param messageProcessingThreadPool a {@code MessageProcessingThreadPool} for message processing.
     */
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

    /**
     * Closes this {@code InternalBaseClient} instance by:
     *     1. Closing all associated {@link WaltzNetworkClient}s.
     *     2. Closing all associated {@link Partition}s.
     *     3. Releasing other internal resources.
     */
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

    /**
     * Sets the given partition ids for this {@code InternalBaseClient} instance to work with.
     * Partitions not in {@code partitionIds} will become inaccessible from this client.
     * To use this method the client must set the {@code client.autoMount} configuration parameter to {@code false}.
     *
     * @param partitionIds the active partitions' ids.
     * @throws InvalidOperationException if {@link InternalBaseClient#autoMount} is {@code true}.
     */
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

    /**
     * Returns ids of all active partitions of this client.
     *
     * @return a set of all active partitions' ids.
     */
    public Set<Integer> getActivePartitions() {
        return new HashSet<>(activePartitions);
    }

    /**
     * A {@link WaltzNetworkClientCallbacks} callback method invoked when mounting a partition.
     * Implemented by the subclasses of {@link InternalBaseClient}.
     *
     * @param networkClient the {@code WaltzNetworkClient} being used to mount the partition.
     * @param partition the {@code Partition} being mounted.
     */
    @Override
    public abstract void onMountingPartition(WaltzNetworkClient networkClient, Partition partition);

    /**
     * Implements {@link WaltzNetworkClientCallbacks#onNetworkClientDisconnected(WaltzNetworkClient)}.
     * Asynchronously attempts to recover from the connection loss by replacing the old {@code closedNetworkClient}
     * with a new {@link WaltzNetworkClient}.
     *
     * @param closedNetworkClient the {@code WaltzNetworkClient} that was disconnected/closed.
     */
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

    /**
     * Not supported on {@code InternalBaseClient}.
     *
     * @throws UnsupportedOperationException if invoked.
     */
    @Override
    public void onTransactionReceived(long transactionId, int header, ReqId reqId) {
        throw new UnsupportedOperationException("not supported by " + this.getClass().getSimpleName());
    }

    /**
     * Implements {@link ManagedClient#setClusterName(String)}.
     * Sets the cluster name.
     *
     * @param clusterName the cluster name to set to.
     */
    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Implements {@link ManagedClient#setClientId(int)}.
     * Sets the client id.
     *
     * @param clientId the client id to set to.
     */
    @Override
    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    /**
     * Implements {@link ManagedClient#setNumPartitions(int)}.
     *
     * Sets the number of partitions, if not set already.
     * If {@link InternalBaseClient#autoMount} is {@code true}, activates all partitions.
     *
     * @param numPartitions the number of partitions.
     * @throws UnsupportedOperationException if {@link #numPartitions} is already set,
     *                                       but is not equal to the parameter {@code numPartitions}.
     */
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

    /**
     * Implements {@link ManagedClient#setEndpoints(Map)}.
     * Sets {@link InternalBaseClient#endpoints} and asynchronously mounts actual partitions.
     *
     * @param endpoints A map of {@link Endpoint} to partition infos representing servers
     *                  and the partitions owned by each of them.
     */
    @Override
    public void setEndpoints(Map<Endpoint, List<PartitionInfo>> endpoints) {
        logger.debug("submitting setEndpoints task");

        // Save endpoints. No need to make a copy of map since ClusterManager always creates a new map before calling setEndpoints().
        this.endpoints = endpoints;

        submitAsyncTask(this::doSetEndpoints);
    }

    /**
     * Asynchronously removes the server by invoking {@link WaltzNetworkClient#close()} on the corresponding network client.
     *
     * @param endpoint the {@code Endpoint} of the server to be removed.
     */
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

    /**
     * @return the clientId.
     */
    public int clientId() {
        return clientId;
    }

    /**
     * @return the clusterName.
     */
    public String clusterName() {
        return clusterName;
    }

    /**
     * Flushes pending transactions by invoking {@link Partition#flushTransactionsAsync()} for each partition.
     * Blocks until the flushing is complete or {@link #running} is {@code false}, whichever happens earlier.
     */
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

    /**
     * Invokes {@link Partition#nudgeWaitingTransactions(long)} for each partition
     * to nudge any transactions pending for more than {@code longWaitThreshold} millis.
     *
     * @param longWaitThreshold the wait time threshold for transactions.
     */
    public void nudgeWaitingTransactions(long longWaitThreshold) {
        for (Partition partition : partitions.values()) {
            partition.nudgeWaitingTransactions(longWaitThreshold);
        }
    }

    /**
     * @return {@code true}, if at least one of the partitions has pending transactions. {@code false}, otherwise.
     */
    public boolean hasPendingTransactions() {
        for (Partition partition : partitions.values()) {
            if (partition.hasPendingTransactions()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the corresponding {@link Partition} object for a given partition id.
     *
     * @param partitionId the id to get the {@code Partition} object for.
     * @return the corresponding {@code Partition} object.
     * @throws PartitionNotFoundException if no Partition exists for such partition id.
     * @throws ClientClosedException if {@link InternalBaseClient} is not running.
     */
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

    /**
     * Checks the connectivity to the server from the given server endpoint network client.
     * @param endpoint The server {@link Endpoint}
     * @return Completable future with the connection status of the given server endpoint.
     */
    public CompletableFuture<Optional<Map<String, Boolean>>> checkServerConnectivity(Endpoint endpoint) {
        WaltzNetworkClient networkClient = getNetworkClient(endpoint);
        return networkClient.checkServerToStorageConnectivity();
    }

}
