package com.wepay.waltz.client.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.internal.network.WaltzNetworkClient;
import com.wepay.waltz.client.internal.network.WaltzNetworkClientCallbacks;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.FlushRequest;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.ClientClosedException;
import com.wepay.waltz.exception.DataChecksumException;
import com.wepay.waltz.exception.PartitionInactiveException;
import com.wepay.zktools.clustermgr.Endpoint;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {

    private static final Logger logger = Logging.getLogger(Partition.class);

    private static final Long[] EMPTY_LONG_ARRAY = new Long[0];
    private static final int MAX_DATA_ATTEMPTS = 5;

    private enum PartitionState {
        ACTIVE, INACTIVE, CLOSED
    }

    public final int partitionId;
    public final int clientId;

    private final Object lock = new Object();
    private final Object transactionApplicationLock = new Object();
    private final TransactionMonitor transactionMonitor;
    private final PriorityQueue<LockFailure> lockFailureQueue;
    private final LinkedList<FlushPoint> flushPointQueue = new LinkedList<>();
    private final HashMap<Long, DataFuture> dataFutures;

    private final AtomicInteger seqNumGenerator = new AtomicInteger(0);

    private final AtomicLong clientHighWaterMark;
    private volatile WaltzNetworkClient networkClient;
    private volatile int generation;
    private volatile PartitionState state = PartitionState.INACTIVE;
    private volatile boolean mounted = false;

    public Partition(int partitionId, int clientId) {
        this.partitionId = partitionId;
        this.clientId = clientId;
        this.generation = -1;
        this.transactionMonitor = new TransactionMonitor();
        this.lockFailureQueue = new PriorityQueue<>(LockFailure.COMPARATOR);
        this.networkClient = null;
        this.clientHighWaterMark = new AtomicLong(-1);
        this.dataFutures = new HashMap<>();
    }

    public void close() {
        synchronized (lock) {
            this.networkClient = null;
            this.state = PartitionState.CLOSED;
            this.mounted = false;
            this.transactionMonitor.close();

            synchronized (dataFutures) {
                if (!dataFutures.isEmpty()) {
                    ClientClosedException exception = new ClientClosedException();

                    for (CompletableFuture<byte[]> future : dataFutures.values()) {
                        future.completeExceptionally(exception);
                    }
                }
            }
            lock.notifyAll();
        }
    }

    public void generation(int generation) {
        synchronized (lock) {
            if (this.generation < generation) {
                this.generation = generation;
            }
        }
    }

    public int generation() {
        return generation;
    }

    public void activate(long highWaterMark) {
        synchronized (lock) {
            if (transactionMonitor.start()) {
                clientHighWaterMark.set(highWaterMark);
                state = PartitionState.ACTIVE;
            }
        }
    }

    public void deactivate() {
        if (transactionMonitor.stop()) {
            // Wait until the transaction monitor becomes empty (no outstanding append request).
            TransactionFuture future = flushTransactionsAsyncInternal();
            while (transactionMonitor.isStopped() && future != null) {
                future.awaitFlush();
                future = flushTransactionsAsyncInternal();
            }
        }

        // Clean up lockFailureQueue and flushPointQueue
        processAuxilliaryQueues();

        synchronized (lock) {
            if (transactionMonitor.isStopped()) {
                state = PartitionState.INACTIVE;
            }
        }
    }

    public boolean isActive() {
        return state == PartitionState.ACTIVE;
    }

    public Endpoint endPoint() {
        // Cache the network client in the local variable for safety
        WaltzNetworkClient networkClient = this.networkClient;

        return networkClient != null ? networkClient.endpoint : null;
    }

    public void mounting(WaltzNetworkClient networkClient) {
        synchronized (lock) {
            logger.debug("mounting partition: {}", this);
            this.mounted = false;
            this.networkClient = networkClient;
            lock.notifyAll();
        }
    }

    public void mounted(WaltzNetworkClient networkClient) {
        synchronized (lock) {
            // Make sure the network client is the right one
            if (this.networkClient == networkClient) {
                logger.debug("partition mounted: {}", this);
                this.mounted = true;
                lock.notifyAll();
            }
        }

        flushTransactionsAsyncInternal();
    }

    public void unmounted(WaltzNetworkClient networkClient) {
        synchronized (lock) {
            // Make sure the network client is the right one
            if (this.networkClient == networkClient) {
                logger.debug("partition unmounted: {}", this);
                this.mounted = false;
                this.networkClient = null;
                lock.notifyAll();
            }
        }
    }

    public void ensureMounted() {
        if (!mounted) {
            synchronized (lock) {
                while (state != PartitionState.CLOSED && !mounted) {
                    if (transactionMonitor.isStopped()) {
                        throw new PartitionInactiveException(partitionId);
                    }

                    try {
                        lock.wait();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }
            }
        }
    }

    public ReqId nextReqId() {
        return new ReqId(clientId, generation, partitionId, seqNumGenerator.incrementAndGet());
    }

    public long clientHighWaterMark() {
        return clientHighWaterMark.get();
    }

    private void processAuxilliaryQueues() {
        synchronized (lockFailureQueue) {
            LockFailure lockFailure;
            while ((lockFailure = lockFailureQueue.peek()) != null) {
                if (lockFailure.transactionId <= clientHighWaterMark.get()) {
                    lockFailureQueue.poll();
                    transactionMonitor.abort(lockFailure.reqId);
                } else {
                    break;
                }
            }
        }

        synchronized (flushPointQueue) {
            FlushPoint fp = flushPointQueue.peek();
            while (fp != null && clientHighWaterMark.get() >= fp.transactionId) {
                flushPointQueue.poll();
                logger.debug("completing flush (deferred): {} reqId={}", this, fp.reqId);
                transactionMonitor.flush(fp.reqId);
                fp = flushPointQueue.peek();
            }
        }
    }

    public void applyTransaction(long transactionId, int header, ReqId reqId, WaltzNetworkClientCallbacks networkClientCallbacks) {
        try {
            if (state != PartitionState.ACTIVE) {
                return;
            }

            synchronized (transactionApplicationLock) {
                long expectedTransactionId = clientHighWaterMark() + 1;

                // Process the transaction only when it has the expected transaction id
                if (expectedTransactionId == transactionId) {
                    transactionMonitor.committed(reqId);

                    networkClientCallbacks.onTransactionReceived(transactionId, header, reqId);
                    // The transaction is successfully applied to the application state
                    // Increment the client high-water mark
                    clientHighWaterMark.incrementAndGet();

                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("unexpected transaction received in applyTransaction, ignoring:"
                            + " transactionId=" + transactionId + " expected=" + expectedTransactionId);
                    }
                }
            }
        } finally {
            processAuxilliaryQueues();
        }
    }

    public TransactionFuture append(AppendRequest request) {
        ensureMounted();

        TransactionFuture future;

        // Serialize transaction requests
        synchronized (transactionMonitor) {
            ReqId reqId = request.reqId;
            future = transactionMonitor.register(reqId, 10000);

            if (future == null) {
                // Transaction registration timed out
                return null;
            }

            // If the future is completed before sending the request, don't send the request. It means a reqId collision.
            if (!future.isDone()) {
                WaltzNetworkClient networkClient = this.networkClient; // for safety

                // Send an append message only when the partition is active.
                if (networkClient != null) {
                    networkClient.sendMessage(request);

                } else {
                    // Failed to send the message. Abort the transaction.
                    transactionMonitor.abort(reqId);
                }
            }
        }

        return future;
    }

    public void nudgeWaitingTransactions(long longWaitThreshold) {
        long lastEnqueueTime = transactionMonitor.lastEnqueuedTime();
        if (lastEnqueueTime >= 0 && System.currentTimeMillis() > lastEnqueueTime + longWaitThreshold) {
            flushTransactionsAsync();
        }
    }

    public TransactionFuture flushTransactionsAsync() {
        ensureMounted();
        return flushTransactionsAsyncInternal();
    }

    private TransactionFuture flushTransactionsAsyncInternal() {
        logger.debug("flushing transactions: {}", this);

        TransactionFuture future = transactionMonitor.lastEnqueued();

        if (future != null && !future.isFlushed() && state != PartitionState.CLOSED) {
            // Send a flush request
            WaltzNetworkClient networkClient = this.networkClient; // for safety
            if (networkClient != null) {
                logger.debug("sending FlushRequest: {} reqId={}", this, future.reqId);
                networkClient.sendMessage(new FlushRequest(future.reqId));
            } else {
                logger.debug("failed to send FlushRequest: {} reqId={}", this, future.reqId);
            }

            return future;

        } else {
            // We don't have any pending transaction, or we are shutting down.
            logger.debug("nothing to flush: {}", this);
            return null;
        }
    }

    public Future<byte[]> getTransactionData(long transactionId) {
        return getTransactionData(transactionId, 1);
    }

    private DataFuture getTransactionData(long transactionId, int attempts) {
        DataFuture future;

        synchronized (dataFutures) {
            future = dataFutures.get(transactionId);
            if (future != null) {
                return future;
            } else {
                future = new DataFuture(attempts);

                if (state == PartitionState.CLOSED) {
                    future.completeExceptionally(new ClientClosedException());
                    return future;
                }

                dataFutures.put(transactionId, future);
            }
        }

        sendTransactionDataRequest(transactionId);

        return future;
    }

    public void resubmitTransactionDataRequests() {
        Long[] pendingRequests;

        synchronized (dataFutures) {
            pendingRequests = dataFutures.keySet().toArray(EMPTY_LONG_ARRAY);
        }

        for (long transactionId : pendingRequests) {
            sendTransactionDataRequest(transactionId);
        }
    }

    public void transactionDataReceived(long transactionId, byte[] data, int checksum, Throwable throwable) {
        DataFuture future;

        synchronized (dataFutures) {
            future = dataFutures.remove(transactionId);
        }

        if (future != null) {
            if (data != null) {
                if (checksum == Utils.checksum(data)) {
                    future.complete(data);
                } else {
                    if (future.attempts < MAX_DATA_ATTEMPTS) {
                        String msg = "transaction data checksum error, retrying... : partitionId = " + partitionId + " transactionId=" + transactionId;
                        logger.warn(msg);
                        getTransactionData(transactionId, future.attempts + 1);
                    } else {
                        String msg = "transaction data checksum error: partitionId = " + partitionId + " transactionId=" + transactionId;
                        logger.error(msg);
                        future.completeExceptionally(new DataChecksumException(msg));
                    }
                }
            } else {
                if (throwable == null) {
                    logger.error("null throwable");
                    future.completeExceptionally(new NullPointerException("null throwable"));
                } else {
                    future.completeExceptionally(throwable);
                }
            }
        }
    }

    public void flushCompleted(ReqId reqId, long transactionId) {
        if (clientHighWaterMark.get() >= transactionId) {
            logger.debug("completing flush: {} reqId={}", this, reqId);
            transactionMonitor.flush(reqId);
        } else {
            logger.debug("deferring completion of flush: {} reqId={}", this, reqId);
            synchronized (flushPointQueue) {
                flushPointQueue.offer(new FlushPoint(transactionId, reqId));
            }
        }
    }

    public void lockFailed(LockFailure lockFailure) {
        if (clientHighWaterMark.get() >= lockFailure.transactionId) {
            transactionMonitor.abort(lockFailure.reqId);
        } else {
            synchronized (lockFailureQueue) {
                lockFailureQueue.offer(lockFailure);
            }
        }
    }

    public boolean hasPendingTransactions() {
        return transactionMonitor.registeredCount() > 0;
    }

    private void sendTransactionDataRequest(long transactionId) {
        // TransactionDataRequest is a RPC request. The partition doesn't need to be mounted.
        if (state != PartitionState.CLOSED) {
            // Cache the network client in the local variable for safety
            WaltzNetworkClient networkClient = this.networkClient;

            if (networkClient != null) {
                networkClient.requestTransactionData(nextReqId(), transactionId);
            } else {
                logger.debug("failed to send data request: {}", this);
            }
        }
    }

    public static class FlushPoint {
        final long transactionId;
        final ReqId reqId;

        FlushPoint(long transactionId, ReqId reqId) {
            this.transactionId = transactionId;
            this.reqId = reqId;
        }
    }

    public String toString() {
        return "partitionId=" + partitionId + " clientId=" + clientId;
    }

    private static class DataFuture extends CompletableFuture<byte[]> {
        final int attempts;

        DataFuture(int attempts) {
            super();
            this.attempts = attempts;
        }

    }

}
