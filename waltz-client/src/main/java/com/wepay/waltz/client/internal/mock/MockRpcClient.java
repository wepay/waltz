package com.wepay.waltz.client.internal.mock;

import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.zktools.clustermgr.Endpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A mock implementation of {@link RpcClient}.
 */
class MockRpcClient implements RpcClient {

    private final Map<Integer, MockClientPartition> partitions;

    MockRpcClient(int clientId, int maxConcurrentTransactions, Map<Integer, MockServerPartition> partitions) {
        this.partitions = MockClientPartition.createForRpcClient(clientId, maxConcurrentTransactions, partitions);

        for (MockClientPartition partition : this.partitions.values()) {
            partition.activate();
        }
    }

    @Override
    public void close() {
        partitions.values().forEach(MockClientPartition::close);
    }

    @Override
    public Future<byte[]> getTransactionData(int partitionId, long transactionId) {
        return CompletableFuture.completedFuture(partitions.get(partitionId).getTransactionData(transactionId));
    }

    @Override
    public Future<Long> getHighWaterMark(int partitionId) {
        return CompletableFuture.completedFuture(partitions.get(partitionId).getHighWaterMark());
    }

    @Override
    public CompletableFuture<Map<Endpoint, Map<String, Boolean>>> checkServerConnections(Set<Endpoint> serverEndpoints) {
        return CompletableFuture.completedFuture(new HashMap<>());
    }

    @Override
    public Future<Object> getServerPartitionAssignments(Endpoint serverEndpoint) {
        return CompletableFuture.completedFuture(new ArrayList<>());
    }

}
