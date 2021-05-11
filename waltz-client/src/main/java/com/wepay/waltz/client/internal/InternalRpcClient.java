package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.internal.network.WaltzNetworkClient;
import com.wepay.zktools.clustermgr.Endpoint;
import io.netty.handler.ssl.SslContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * An internal implementation of {@link RpcClient}, extending {@link InternalBaseClient}.
 */
public class InternalRpcClient extends InternalBaseClient implements RpcClient {

    /**
     * Class Constructor, automatically mounts all partitions.
     *
     * @param sslCtx the {@link SslContext}
     * @param maxConcurrentTransactions the maximum number of concurrent transactions.
     * @param callbacks a {@link WaltzClientCallbacks} instance.
     */
    public InternalRpcClient(SslContext sslCtx, int maxConcurrentTransactions, WaltzClientCallbacks callbacks) {
        // InternalRpcClient always mounts all partition
        super(true, sslCtx, maxConcurrentTransactions, callbacks, null);
    }

    /**
     * Invoked when a {@link Partition} is being mounted.
     * Resubmits outstanding transaction data requests, if any.
     * Resubmits high watermark requests, if any.
     *
     * @param networkClient the {@code WaltzNetworkClient} being used to mount the partition.
     * @param partition the {@code Partition} being mounted.
     */
    @Override
    public void onMountingPartition(final WaltzNetworkClient networkClient, final Partition partition) {
        partition.resubmitTransactionDataRequests();
        partition.resubmitHighWaterMarkRequests();
    }

    /**
     * Gets transaction data of a given transaction id from a given partition id.
     *
     * @param partitionId the id of the partition to read from.
     * @param transactionId the id of the transaction to read.
     * @return a {@link Future} which contains the serialized transaction data when complete.
     */
    @Override
    public Future<byte[]> getTransactionData(int partitionId, long transactionId) {
        return getPartition(partitionId).getTransactionData(transactionId);
    }

    /**
     * Gets high watermark from a given partition id.
     *
     * @param partitionId the id of the partition to read from.
     * @return a {@link Future} which contains the high watermark when complete.
     */
    @Override
    public Future<Long> getHighWaterMark(int partitionId) {
        return getPartition(partitionId).getHighWaterMark();
    }

    /**
     * Checks the connectivity
     * 1. to the given server endpoints and also
     * 2. from each server endpoint to the storage nodes within the cluster.
     * Waits for all the completable futures to complete.
     *
     * @param serverEndpoints Set of server endpoints.
     * @return a combined completable future with the connection status per server endpoint.
     * @throws InterruptedException if thrown by the {@code WaltzNetworkClient}
     */
    @Override
    public Future<Map<Endpoint, Map<String, Boolean>>> checkServerConnections(Set<Endpoint> serverEndpoints) throws InterruptedException {
        Map<Endpoint, CompletableFuture<Map<String, Boolean>>> futures = new HashMap<>();
        for (Endpoint endpoint : serverEndpoints) {
            WaltzNetworkClient networkClient = getNetworkClient(endpoint);
            CompletableFuture<Map<String, Boolean>> future = networkClient.checkServerToStorageConnectivity();
            futures.put(endpoint, future);
        }

        return CompletableFuture
                .allOf(futures.values().toArray(new CompletableFuture[futures.size()]))
                .handle((v, t) -> {
                    Map<Endpoint, Map<String, Boolean>> connectivityStatusMap = new HashMap<>();
                    futures
                        .entrySet()
                        .stream()
                        .filter(entry -> !entry.getValue().isCompletedExceptionally())
                        .forEach((entry) -> connectivityStatusMap.put(entry.getKey(), entry.getValue().join()));
                    return connectivityStatusMap;
                });
    }

    /**
     * Gets the list of partitions assigned to the server with serverEndpoint
     *
     * @param serverEndpoint Server from which to fetch the assigned partitions
     * @return Future which will contain the list of partitions when complete
     * @throws InterruptedException if thrown by the {@code WaltzNetworkClient}
     */
    @Override
    public Future<List<Integer>> getServerPartitionAssignments(Endpoint serverEndpoint) throws InterruptedException {
        WaltzNetworkClient networkClient = getNetworkClient(serverEndpoint);
        return networkClient.getServerPartitionAssignments();
    }

    /**
     * Adds the given partition Id as a preferred partition to the given Server Endpoint.
     * @param serverEndpoint Server Endpoint to add the partition to.
     * @param partitionIds The list of partition Ids.
     * @return a Future which will complete with a {@code true} if the preferred partition is added
     * successfully, a {@code false} otherwise, or an exception if any.
     * @throws InterruptedException If thread is interrupted while waiting for Network client channel to be ready.
     */
    @Override
    public Future<Boolean> addPreferredPartition(Endpoint serverEndpoint, List<Integer> partitionIds) throws InterruptedException {
        WaltzNetworkClient networkClient = getNetworkClient(serverEndpoint);
        return networkClient.addPreferredPartition(partitionIds);
    }

    /**
     * Removes the given partition Id as a preferred partition from the given Server Endpoint.
     * @param serverEndpoint Server Endpoint from which the partition is to be removed.
     * @param partitionId The partition Id.
     * @return a Future which will complete with a {@code true} if the preferred partition is added
     * successfully, a {@code false} otherwise, or an exception if any.
     * @throws InterruptedException If thread is interrupted while waiting for Network client channel to be ready.
     */
    @Override
    public Future<Boolean> removePreferredPartition(Endpoint serverEndpoint,
                                                    int partitionId) throws InterruptedException {
        WaltzNetworkClient networkClient = getNetworkClient(serverEndpoint);
        return networkClient.removePreferredPartition(partitionId);
    }
}
