package com.wepay.waltz.client.internal;

import com.wepay.zktools.clustermgr.Endpoint;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * The interface for implementations of RPC clients to communicate with Waltz cluster.
 */
public interface RpcClient {

    void close();

    Future<byte[]> getTransactionData(int partitionId, long transactionId);

    Future<Long> getHighWaterMark(int partitionId);

    Future<Map<Endpoint, Map<String, Boolean>>> checkServerConnections(Set<Endpoint> serverEndpoints) throws InterruptedException;

    Future<List<Integer>> getServerPartitionAssignments(Endpoint serverEndpoint) throws InterruptedException;

    Future<Boolean> addPreferredPartition(Endpoint serverEndpoint, int partitionId) throws InterruptedException;

    Future<Boolean> removePreferredPartition(Endpoint serverEndpoint, int partitionId) throws InterruptedException;
}
