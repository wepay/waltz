package com.wepay.waltz.client.internal;

import com.wepay.zktools.clustermgr.Endpoint;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * The interface for implementations of RPC clients to communicate with Waltz cluster.
 */
public interface RpcClient {

    void close();

    Future<byte[]> getTransactionData(int partitionId, long transactionId);

    Future<Long> getHighWaterMark(int partitionId);

    CompletableFuture<Map<Endpoint, Map<String, Boolean>>>  checkServerConnections(Set<Endpoint> serverEndpoints);
}
