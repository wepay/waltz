package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.internal.network.WaltzNetworkClient;
import io.netty.handler.ssl.SslContext;

import java.util.concurrent.Future;

public class InternalRpcClient extends InternalBaseClient implements RpcClient {

    public InternalRpcClient(SslContext sslCtx, int maxConcurrentTransactions, WaltzClientCallbacks callbacks) {
        // InternalRpcClient always mounts all partition
        super(true, sslCtx, maxConcurrentTransactions, callbacks, null);
    }

    @Override
    public void onMountingPartition(final WaltzNetworkClient networkClient, final Partition partition) {
        // re-submit outstanding requests if any
        partition.resubmitTransactionDataRequests();
    }

    @Override
    public Future<byte[]> getTransactionData(int partitionId, long transactionId) {
        return getPartition(partitionId).getTransactionData(transactionId);
    }

}
