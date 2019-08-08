package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.internal.network.WaltzNetworkClient;
import io.netty.handler.ssl.SslContext;

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
     *
     * @param networkClient the {@code WaltzNetworkClient} being used to mount the partition.
     * @param partition the {@code Partition} being mounted.
     */
    @Override
    public void onMountingPartition(final WaltzNetworkClient networkClient, final Partition partition) {
        partition.resubmitTransactionDataRequests();
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

}
