package com.wepay.waltz.client.internal;

import java.util.concurrent.Future;

/**
 * The interface for implementations of RPC clients to communicate with Waltz cluster.
 */
public interface RpcClient {

    void close();

    Future<byte[]> getTransactionData(int partitionId, long transactionId);

    Future<Long> getHighWaterMark(int partitionId);

}
