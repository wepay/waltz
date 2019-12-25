package com.wepay.waltz.client.internal.mock;

import com.wepay.riff.util.RepeatingTask;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.client.internal.TransactionFuture;
import com.wepay.waltz.client.internal.TransactionMonitor;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.FlushResponse;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.exception.PartitionInactiveException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Client's mock representation of a partition.
 */
final class MockClientPartition {

    private final int clientId;
    private final MockServerPartition serverPartition;
    private final WaltzClientCallbacks callbacks;
    private final MessageReader messageReader;
    private final TransactionMonitor transactionMonitor;
    private final AtomicLong clientHighWaterMark;
    private final AtomicInteger seqNumGenerator = new AtomicInteger(0);
    private final StreamTask task;

    private boolean active;

    private MockClientPartition(int clientId,
                                int maxConcurrentTransactions,
                                MockServerPartition serverPartition,
                                WaltzClientCallbacks callbacks,
                                RpcClient rpcClient
    ) {
        this.clientId = clientId;
        this.serverPartition = serverPartition;
        this.callbacks = callbacks;
        this.messageReader = serverPartition.getMessageReader();
        this.transactionMonitor = new TransactionMonitor(maxConcurrentTransactions);
        this.clientHighWaterMark = new AtomicLong(-1L);
        this.task = new StreamTask(messageReader, rpcClient);
        this.task.suspendFeed();
        this.task.start();
    }

    private MockClientPartition(int clientId, int maxConcurrentTransactions, MockServerPartition serverPartition) {
        this.clientId = clientId;
        this.serverPartition = serverPartition;
        this.callbacks = null;
        this.messageReader = null;
        this.transactionMonitor = new TransactionMonitor(maxConcurrentTransactions);
        this.clientHighWaterMark = null;
        this.task = null;
    }

    void close() {
        if (task != null) {
            CompletableFuture<Boolean> future = task.stop();
            while (true) {
                try {
                    future.get();
                    return;
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                } catch (Exception ex) {
                    //
                }
            }
        }
    }

    void activate() {
        synchronized (this) {
            if (transactionMonitor != null && transactionMonitor.start()) {
                if (clientHighWaterMark != null) {
                    clientHighWaterMark.set(callbacks.getClientHighWaterMark(serverPartition.partitionId));
                }
                active = true;
                if (task != null) {
                    task.resumeFeed();
                }
            }
        }
    }

    void deactivate() {
        synchronized (this) {
            if (transactionMonitor.stop()) {
                active = false;
                if (task != null) {
                    task.suspendFeed();
                }
            }
        }
    }

    boolean isActive() {
        return !transactionMonitor.isStopped();
    }

    void ensureActive() {
        if (transactionMonitor.isStopped()) {
            throw new PartitionInactiveException(serverPartition.partitionId);
        }
    }

    TransactionFuture append(AppendRequest request, TransactionContext context, boolean forceFailure, boolean forceLockFailure) {
        TransactionFuture future;

        // Serialize transaction requests
        synchronized (transactionMonitor) {
            ReqId reqId = request.reqId;
            future = transactionMonitor.register(reqId, context, 10000);

            if (future == null) {
                // Transaction registration timed out
                return null;
            }

            // If the future is completed before sending the request, don't send the request. It means a reqId collision.
            if (!future.isDone()) {
                if (task.isRunning()) {
                    serverPartition.append(request, forceFailure, forceLockFailure);
                } else {
                    // Failed to send the message. Abort the transaction.
                    transactionMonitor.abort(reqId);
                }
            }
        }

        return future;
    }

    byte[] getTransactionData(long transactionId) {
        return serverPartition.getTransactionData(transactionId);
    }

    Long getHighWaterMark() {
        return serverPartition.getHighWaterMark();
    }

    void flushTransactions() {
        synchronized (this) {
            if (active) {
                TransactionFuture last = transactionMonitor.lastEnqueued();
                if (last != null) {
                    serverPartition.flushTransactions(last.reqId);
                }
            }
        }
    }

    void nudgeWaitingTransactions(long longWaitThreshold) {
        synchronized (this) {
            if (active) {
                long lastEnqueueTime = transactionMonitor.lastEnqueuedTime();
                if (lastEnqueueTime >= 0 && System.currentTimeMillis() > lastEnqueueTime + longWaitThreshold) {
                    flushTransactions();
                }
            }
        }
    }

    ReqId nextReqId() {
        return new ReqId(clientId, 0, serverPartition.partitionId, seqNumGenerator.incrementAndGet());
    }

    void suspendFeed() {
        task.suspendFeed();
    }

    void resumeFeed() {
        task.resumeFeed();
    }

    private class StreamTask extends RepeatingTask {

        private final MessageReader messageReader;
        private final RpcClient rpcClient;
        private final Map<ReqId, TransactionContext> transactionApplicationFailed = new HashMap<>();

        private boolean suspended = false;

        StreamTask(MessageReader messageReader, RpcClient rpcClient) {
            super("MockClient");
            this.messageReader = messageReader;
            this.rpcClient = rpcClient;
        }

        void suspendFeed() {
            synchronized (this) {
                suspended = true;
            }
        }

        void resumeFeed() {
            synchronized (this) {
                suspended = false;
                this.notifyAll();
            }
        }

        @Override
        public CompletableFuture<Boolean> stop() {
            synchronized (this) {
                notifyAll();
                return super.stop();
            }
        }

        @Override
        public void task() throws Exception {
            AbstractMessage message = messageReader.nextMessage(1000);

            synchronized (this) {
                while (isRunning() && suspended) {
                    try {
                        this.wait();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }

                    if (!isRunning()) {
                        return;
                    }
                }
            }

            if (message != null) {
                if (message instanceof FeedData) {
                    FeedData feedData = (FeedData) message;

                    while (true) {
                        if (feedData.transactionId == clientHighWaterMark.get() + 1) {
                            TransactionContext context = transactionMonitor.committed(feedData.reqId);

                            if (context != null) {
                                context.onCompletion(true);
                            } else {
                                context = transactionApplicationFailed.remove(feedData.reqId);
                            }

                            Transaction transaction = new Transaction(
                                feedData.transactionId,
                                feedData.header,
                                feedData.reqId,
                                rpcClient
                            );

                            try {
                                callbacks.applyTransaction(transaction);
                                clientHighWaterMark.incrementAndGet();

                            } catch (Exception ex) {
                                if (context != null) {
                                    transactionApplicationFailed.put(transaction.reqId, context);
                                }

                                try {
                                    callbacks.uncaughtException(transaction.reqId.partitionId(), transaction.transactionId, ex);
                                } catch (Throwable t) {
                                    // Ignore
                                }

                                continue;
                            }

                            if (context != null) {
                                context.onApplication();
                            }
                        }
                        break;
                    }

                } else if (message instanceof FlushResponse) {
                    FlushResponse flushResponse = (FlushResponse) message;

                    if (flushResponse.transactionId <= clientHighWaterMark.get()) {
                        transactionMonitor.flush(flushResponse.reqId);
                    } else {
                        throw new IllegalStateException("flush out of order");
                    }

                } else if (message instanceof LockFailure) {
                    LockFailure lockFailure = (LockFailure) message;

                    TransactionContext context = transactionMonitor.getTransactionContext(lockFailure.reqId);
                    if (context != null) {
                        context.onLockFailure();
                    }

                    if (lockFailure.transactionId <= clientHighWaterMark.get()) {
                        transactionMonitor.abort(lockFailure.reqId);
                    } else {
                        throw new IllegalStateException("lock failure out of order");
                    }

                } else {
                    throw new UnsupportedOperationException("unsupported message: " + message);
                }
            }
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            ex.printStackTrace();
        }

    }

    static Map<Integer, MockClientPartition> createForStreamClient(
        int clientId,
        int maxConcurrentTransactions,
        Map<Integer, MockServerPartition> serverPartitions,
        WaltzClientCallbacks callbacks,
        RpcClient rpcClient
    ) {
        HashMap<Integer, MockClientPartition> partitions = new HashMap<>(serverPartitions.size());

        for (Map.Entry<Integer, MockServerPartition> entry : serverPartitions.entrySet()) {
            partitions.put(entry.getKey(), new MockClientPartition(clientId, maxConcurrentTransactions, entry.getValue(), callbacks, rpcClient));
        }

        return partitions;
    }

    static Map<Integer, MockClientPartition> createForRpcClient(
        int clientId,
        int maxConcurrentTransactions,
        Map<Integer, MockServerPartition> serverPartitions
    ) {
        HashMap<Integer, MockClientPartition> partitions = new HashMap<>(serverPartitions.size());

        for (Map.Entry<Integer, MockServerPartition> entry : serverPartitions.entrySet()) {
            partitions.put(entry.getKey(), new MockClientPartition(clientId, maxConcurrentTransactions, entry.getValue()));
        }

        return partitions;
    }

}
