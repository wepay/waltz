package com.wepay.waltz.client.internal.mock;

import com.wepay.waltz.client.internal.RpcClient;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

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

}
