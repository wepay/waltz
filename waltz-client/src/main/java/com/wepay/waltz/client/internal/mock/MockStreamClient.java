package com.wepay.waltz.client.internal.mock;

import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.client.internal.StreamClient;
import com.wepay.waltz.client.internal.TransactionBuilderImpl;
import com.wepay.waltz.client.internal.TransactionFuture;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.exception.InvalidOperationException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class MockStreamClient implements StreamClient {

    private final Map<Integer, MockClientPartition> clientPartitions;
    private final WaltzClientCallbacks callbacks;

    private final int clientId;
    private final String clusterName;
    private final boolean autoMount;

    private final Object lock = new Object();
    private int numPendingTransactions = 0;

    private final AtomicBoolean forceNextAppendFail = new AtomicBoolean(false);

    MockStreamClient(int clientId,
                     String clusterName,
                     boolean autoMount,
                     int maxConcurrentTransactions,
                     Map<Integer, MockServerPartition> partitions,
                     RpcClient rpcClient,
                     WaltzClientCallbacks callbacks
    ) {
        this.clientId = clientId;
        this.clusterName = clusterName;
        this.clientPartitions = MockClientPartition.createForStreamClient(clientId, maxConcurrentTransactions, partitions, callbacks, rpcClient);
        this.callbacks = callbacks;
        this.autoMount = autoMount;
        if (autoMount) {
            for (Map.Entry<Integer, MockClientPartition> entry : clientPartitions.entrySet()) {
                entry.getValue().activate();
            }
        }
    }

    @Override
    public void close() {
        clientPartitions.values().forEach(MockClientPartition::close);
    }

    @Override
    public int clientId() {
        return clientId;
    }

    @Override
    public String clusterName() {
        return clusterName;
    }

    @Override
    public TransactionBuilderImpl getTransactionBuilder(TransactionContext context) {
        int partitionId = context.partitionId(clientPartitions.size());

        MockClientPartition partition = clientPartitions.get(partitionId);

        partition.ensureActive();

        return new TransactionBuilderImpl(partition.nextReqId(), callbacks.getClientHighWaterMark(partitionId));
    }

    @Override
    public TransactionFuture append(AppendRequest request) {
        int partitionId = request.reqId.partitionId();

        synchronized (lock) {
            numPendingTransactions++;
        }

        TransactionFuture future;

        try {
            MockClientPartition partition = clientPartitions.get(partitionId);
            while (true) {
                future = partition.append(request, forceNextAppendFail.compareAndSet(true, false));

                if (future == null) {
                    // Transaction registration timed out. Flush pending transactions and retry.
                    partition.flushTransactions();
                } else {
                    break;
                }
            }
        } catch (Exception ex) {
            future = new TransactionFuture(request.reqId);
            future.completeExceptionally(ex);
        }

        future.whenComplete((v, e) -> {
            synchronized (lock) {
                numPendingTransactions--;
                if (numPendingTransactions <= 0) {
                    lock.notifyAll();
                }
            }
        });

        return future;
    }

    @Override
    public void flushTransactions() {
        clientPartitions.values().forEach(MockClientPartition::flushTransactions);

        synchronized (lock) {
            while (numPendingTransactions > 0) {
                try {
                    lock.wait(10000);
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        }
    }

    @Override
    public void nudgeWaitingTransactions(long longWaitThreshold) {
        clientPartitions.values().forEach(partition -> partition.nudgeWaitingTransactions(longWaitThreshold));
    }

    @Override
    public boolean hasPendingTransactions() {
        synchronized (lock) {
            return numPendingTransactions > 0;
        }
    }

    public void setActivePartitions(Set<Integer> partitionIds) {
        synchronized (lock) {
            if (autoMount) {
                throw new InvalidOperationException("failed to set partitions: the client is configured with auto-mount on");
            }

            for (Map.Entry<Integer, MockClientPartition> entry : clientPartitions.entrySet()) {
                if (partitionIds.contains(entry.getKey())) {
                    entry.getValue().activate();
                } else {
                    entry.getValue().deactivate();
                }
            }
        }
    }

    public Set<Integer> getActivePartitions() {
        synchronized (lock) {
            Set<Integer> set = new HashSet<>();
            for (Map.Entry<Integer, MockClientPartition> entry : clientPartitions.entrySet()) {
                if (entry.getValue().isActive()) {
                    set.add(entry.getKey());
                }
            }
            return set;
        }
    }

    void suspendFeed() {
        clientPartitions.values().forEach(MockClientPartition::suspendFeed);
    }

    void resumeFeed() {
        clientPartitions.values().forEach(MockClientPartition::resumeFeed);
    }

    void forceNextAppendFail() {
        forceNextAppendFail.set(true);
    }
}
