package com.wepay.waltz.server.internal;

import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.metrics.core.MetricGroup;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.core.Timer;
import com.wepay.riff.network.Message;
import com.wepay.riff.util.Logging;
import com.wepay.riff.util.RequestQueue;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.FeedRequest;
import com.wepay.waltz.common.message.FeedSuspended;
import com.wepay.waltz.common.message.FlushRequest;
import com.wepay.waltz.common.message.FlushResponse;
import com.wepay.waltz.common.message.HighWaterMarkRequest;
import com.wepay.waltz.common.message.HighWaterMarkResponse;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.MessageType;
import com.wepay.waltz.common.message.MountRequest;
import com.wepay.waltz.common.message.MountResponse;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.message.TransactionDataRequest;
import com.wepay.waltz.common.message.TransactionDataResponse;
import com.wepay.waltz.common.util.QueueConsumerTask;
import com.wepay.waltz.exception.InvalidOperationException;
import com.wepay.waltz.exception.RpcException;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.store.exception.StorePartitionClosedException;
import com.wepay.zktools.util.Uninterruptibly;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of a partition within {@link com.wepay.waltz.server.WaltzServer}.
 */
public class Partition {

    private static final Logger logger = Logging.getLogger(Partition.class);
    private static final MetricRegistry REGISTRY = MetricRegistry.getInstance();

    public final int lockTableSize;
    public final long minFetchSize;
    public final long realtimeThreshold; // > minFetchSize

    public final int partitionId;

    private final StorePartition storePartition;
    private final AppendTask appendTask;
    private final FeedTask nearRealtimeFeedTask;
    private final FeedTask catchupFeedTask;
    private final LinkedList<FeedContext> pausedFeedContexts;
    private final FeedCachePartition feedCachePartition;
    private final FeedSynchronizer feedSync = new FeedSynchronizer();
    private final HashMap<Integer, Long> partitionClientSeqNums = new HashMap<>();
    private final TransactionFetcher transactionFetcher;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CompletableFuture<Boolean> closeFuture = new CompletableFuture<>();

    private final String metricsGroup;

    private Meter transactionMeter;
    private Meter highWaterMarkMeter;
    private Meter transactionRejectionMeter;
    private Timer responseLatencyTimer;
    private volatile long commitHighWaterMark = Long.MIN_VALUE;

    /**
     * Class Constructor.
     * @param partitionId ID of the partition.
     * @param storePartition {@link StorePartition} associated with the given partition ID.
     * @param feedCachePartition {@link FeedCachePartition} associated with the given partition ID.
     * @param transactionFetcher {@link TransactionFetcher} associated with the {@code WaltzServer} to which the partition is part of.
     * @param config the config of the {@code WaltzServer} to which the partition is part of.
     */
    public Partition(int partitionId, StorePartition storePartition, FeedCachePartition feedCachePartition, TransactionFetcher transactionFetcher, WaltzServerConfig config) {
        this.partitionId = partitionId;
        this.lockTableSize = (int) config.get(WaltzServerConfig.OPTIMISTIC_LOCK_TABLE_SIZE);
        this.minFetchSize = (int) config.get(WaltzServerConfig.MIN_FETCH_SIZE);
        this.realtimeThreshold = (int) config.get(WaltzServerConfig.REALTIME_THRESHOLD);
        this.storePartition = storePartition;
        this.appendTask = new AppendTask();
        this.nearRealtimeFeedTask = new FeedTask("R", new PriorityBlockingQueue<>(100, FeedContext.HIGH_WATER_MARK_COMPARATOR));
        this.catchupFeedTask = new FeedTask("C", new LinkedBlockingQueue<>());
        this.pausedFeedContexts = new LinkedList<>();
        this.metricsGroup = String.format("%s.partition-%d", MetricGroup.WALTZ_SERVER_METRIC_GROUP, partitionId);

        // Register metrics
        registerMetrics();

        this.feedCachePartition = feedCachePartition;
        this.transactionFetcher = transactionFetcher;
    }

    /**
     * Opens the partition by starting the {@link AppendTask} and {@link FeedTask} threads.
     * @throws StoreException
     */
    public void open() throws StoreException {
        // Start threads
        appendTask.start();
        nearRealtimeFeedTask.start();
        catchupFeedTask.start();
    }

    /**
     * Asynchronously closes the partition by closing its corresponding {@code StorePartition} and {@link FeedSynchronizer}.
     * @return {@link CompletableFuture} with status as True if all {@code AppendTask} and {@code FeedTask} threads are
     * stopped successfully..
     */
    public CompletableFuture<Boolean> closeAsync() {
        // Un-register metrics
        unregisterMetrics();

        if (running.compareAndSet(true, false)) {
            storePartition.close();

            CompletableFuture<Boolean> f1 = nearRealtimeFeedTask.stop();
            CompletableFuture<Boolean> f2 = catchupFeedTask.stop();
            CompletableFuture<Boolean> f3 = appendTask.stop();

            feedSync.close();

            CompletableFuture.allOf(f1, f2, f3).whenComplete((v, t) -> closeFuture.complete(Boolean.TRUE));
        }
        return closeFuture;
    }

    /**
     * Closes the partition by stopping the {@link AppendTask} and {@link FeedTask} threads that are started when open()
     * was called.
     */
    public void close() {
        CompletableFuture<Boolean> future = closeAsync();
        Uninterruptibly.run(future::get);
    }

    /**
     * Updates the generation in the {@code StorePartition}.
     * @param generation The new generation value of the partition.
     */
    public void generation(int generation) {
        storePartition.generation(generation);
    }

    /**
     * Returns True if the {@code StorePartition} is healthy, otherwise returns False.
     * @return True if the {@code StorePartition} is healthy, otherwise returns False.
     */
    public boolean isHealthy() {
        return storePartition.isHealthy();
    }

    /**
     * Returns True if the partition is closed, otherwise returns False.
     * @return True if the partition is closed, otherwise returns False.
     */
    public boolean isClosed() {
        return !running.get();
    }

    /**
     * Returns total number of realtime {@link FeedContext} added to this partition.
     * @return total number of realtime {@link FeedContext} added to this partition.
     */
    public long getTotalRealtimeFeedContextAdded() {
        return nearRealtimeFeedTask.totalAdded();
    }

    /**
     * Returns total number of realtime {@link FeedContext} removed from this partition.
     * @return total number of realtime {@link FeedContext} removed from this partition.
     */
    public long getTotalRealtimeFeedContextRemoved() {
        return nearRealtimeFeedTask.totalRemoved();
    }

    /**
     * Returns total number of catchup {@link FeedContext} added this partition.
     * @return total number of catchup {@link FeedContext} added this partition.
     */
    public int getTotalCatchupFeedContextAdded() {
        return (int) catchupFeedTask.totalAdded();
    }

    /**
     * Returns total number of catchup {@link FeedContext} removed from this partition.
     * @return total number of catchup {@link FeedContext} removed from this partition.
     */
    public int getTotalCatchupFeedContextRemoved() {
        return (int) catchupFeedTask.totalRemoved();
    }

    /**
     * Processes the request message received by this partition.
     * @param msg The {@link Message} received by this partition.
     * @param client The client that has sent the request to this partition.
     * @throws PartitionClosedException thrown if the partition is closed.
     * @throws StoreException thrown if {@code StorePartition} for this partition is closed.
     */
    public void receiveMessage(Message msg, PartitionClient client) throws PartitionClosedException, StoreException {
        if (!running.get()) {
            throw new PartitionClosedException("already closed");
        }

        switch (msg.type()) {
            case MessageType.MOUNT_REQUEST:
                if (isValid(client)) {
                    // Flush the append queue before starting the feed
                    flushAppendQueue().whenComplete((h, t) -> {
                        initializeFeed((MountRequest) msg, client);
                    });
                }
                break;

            case MessageType.APPEND_REQUEST:
                transactionMeter.mark(); // measures rate of transaction
                if (isValid(client)) {
                    appendTask.enqueue(new AppendContext((AppendRequest) msg, client));
                }
                break;

            case MessageType.FEED_REQUEST:
                if (isValid(client)) {
                    resumeFeed((FeedRequest) msg, client);
                }
                break;

            case MessageType.TRANSACTION_DATA_REQUEST:
                getTransactionData((TransactionDataRequest) msg, client);
                break;

            case MessageType.FLUSH_REQUEST:
                // Flush the append queue
                flushAppendQueue().whenComplete((h, t) -> {
                    client.sendMessage(new FlushResponse(((FlushRequest) msg).reqId, h), true);
                });
                break;

            case MessageType.HIGH_WATER_MARK_REQUEST:
                client.sendMessage(new HighWaterMarkResponse(((HighWaterMarkRequest) msg).reqId, commitHighWaterMark), true);
                break;

            default:
                throw new IllegalArgumentException("message not handled: msg=" + msg);
        }
    }

    /**
     * Assigns a sequence number for the given client and stores this information in a map.
     * @param client The client that is interested in this partition.
     */
    public void setPartitionClient(PartitionClient client) {
        synchronized (partitionClientSeqNums) {
            Long currentSeqNum = partitionClientSeqNums.get(client.clientId());
            if (currentSeqNum == null || currentSeqNum < client.seqNum()) {
                partitionClientSeqNums.put(client.clientId(), client.seqNum());
            }
        }
    }

    /**
     * Removes the client from the list of clients that are part of this partition.
     * @param client The client that has to be removed from this partition.
     */
    public void removePartitionClient(PartitionClient client) {
        synchronized (partitionClientSeqNums) {
            Long currentSeqNum = partitionClientSeqNums.get(client.clientId());
            if (currentSeqNum != null && currentSeqNum.equals(client.seqNum())) {
                partitionClientSeqNums.remove(client.clientId());
            }
        }
    }

    /**
     * Sends out a response to the client if the partition is not found (i.e. if the partition is null).
     * @param msg The request message received from the client.
     * @param client The client that has sent a request to a partition.
     */
    public static void partitionNotFound(Message msg, PartitionClient client) {
        AbstractMessage r = (AbstractMessage) msg;
        ReqId reqId = r.reqId;

        switch (msg.type()) {
            case MessageType.MOUNT_REQUEST:
                // Tell the client that partition is not ready.
                client.sendMessage(new MountResponse(reqId, MountResponse.PartitionState.NOT_READY), true);
                break;

            case MessageType.APPEND_REQUEST:
                break;

            case MessageType.FEED_REQUEST:
                break;

            case MessageType.FLUSH_REQUEST:
                break;

            case MessageType.TRANSACTION_DATA_REQUEST:
                long transactionId = ((TransactionDataRequest) msg).transactionId;
                client.sendMessage(new TransactionDataResponse(reqId, transactionId, new RpcException("partition not ready")), true);
                break;

            default:
                logger.error("partition not found: partitionId=" + reqId.partitionId() + " clientId=" + reqId.clientId() + " msg=" + msg);
        }
    }

    private void registerMetrics() {
        transactionMeter = REGISTRY.meter(metricsGroup, "transaction");
        highWaterMarkMeter = REGISTRY.meter(metricsGroup, "successful-append");
        transactionRejectionMeter = REGISTRY.meter(metricsGroup, "rejected-append");
        responseLatencyTimer = REGISTRY.timer(metricsGroup, "response-latency");
        REGISTRY.gauge(metricsGroup, "pending-append", (Gauge<Integer>) () -> storePartition.numPendingAppends());
        REGISTRY.gauge(metricsGroup, "is-closed", (Gauge<Boolean>) () -> isClosed());
        REGISTRY.gauge(metricsGroup, "generation", (Gauge<Integer>) () -> storePartition.generation());
        REGISTRY.gauge(metricsGroup, "append-queue-size", (Gauge<Integer>) () -> appendTask.queueSize());
        REGISTRY.gauge(metricsGroup, "total-real-time-feed-context-added", (Gauge<Long>) () -> getTotalRealtimeFeedContextAdded());
        REGISTRY.gauge(metricsGroup, "total-real-time-feed-context-removed", (Gauge<Long>) () -> getTotalRealtimeFeedContextRemoved());
        REGISTRY.gauge(metricsGroup, "total-catchup-feed-context-added", (Gauge<Integer>) () -> getTotalCatchupFeedContextAdded());
        REGISTRY.gauge(metricsGroup, "total-catchup-feed-context-removed", (Gauge<Integer>) () -> getTotalCatchupFeedContextRemoved());
        REGISTRY.gauge(metricsGroup, "high-water-mark", (Gauge<Long>) () -> commitHighWaterMark);
    }

    private void unregisterMetrics() {
        REGISTRY.remove(metricsGroup, "transaction");
        REGISTRY.remove(metricsGroup, "successful-append");
        REGISTRY.remove(metricsGroup, "rejected-append");
        REGISTRY.remove(metricsGroup, "response-latency");
        REGISTRY.remove(metricsGroup, "pending-append");
        REGISTRY.remove(metricsGroup, "is-closed");
        REGISTRY.remove(metricsGroup, "generation");
        REGISTRY.remove(metricsGroup, "append-queue-size");
        REGISTRY.remove(metricsGroup, "total-real-time-feed-context-added");
        REGISTRY.remove(metricsGroup, "total-real-time-feed-context-removed");
        REGISTRY.remove(metricsGroup, "total-catchup-feed-context-added");
        REGISTRY.remove(metricsGroup, "total-catchup-feed-context-removed");
        REGISTRY.remove(metricsGroup, "high-water-mark");
    }

    /**
     * Invokes {@link AppendTask#flush()} on the underlying {@link #appendTask},
     * closes the partition if it throws an {@link InvalidOperationException}.
     *
     * @return a {@link CompletableFuture} which will complete with result of the flush operation.
     * @throws PartitionClosedException if this partition is closed.
     */
    CompletableFuture<Long> flushAppendQueue() throws PartitionClosedException {
        try {
            return appendTask.flush();
        } catch (InvalidOperationException e) {
            logger.error(e.getMessage());

            while (!isClosed()) {
                logger.error("Append task queue is closed however the Partition is still open, closing it");
                close();
            }
            throw new PartitionClosedException("already closed");
        }
    }

    private void pauseFeedContext(FeedContext feedContext) throws StoreException {
        synchronized (pausedFeedContexts) {
            pausedFeedContexts.add(feedContext);
        }
        resumePausedFeedContexts();
    }

    /**
     * Resumes the previously paused feed context for this partition.
     * @throws StoreException thrown if {@code StorePartition} for this partition is closed.
     */
    public void resumePausedFeedContexts() throws StoreException {
        synchronized (pausedFeedContexts) {
            Iterator<FeedContext> iter = pausedFeedContexts.iterator();
            while (iter.hasNext()) {
                FeedContext feedContext = iter.next();
                if (feedContext.isActive()) {
                    if (feedContext.isWritable()) {
                        addFeedContext(feedContext);
                        iter.remove();
                    }
                } else {
                    iter.remove();
                }
            }
        }
    }

    private boolean isValid(PartitionClient client) {
        synchronized (partitionClientSeqNums) {
            Long currentSeqNum = partitionClientSeqNums.get(client.clientId());
            return currentSeqNum != null && currentSeqNum.equals(client.seqNum());
        }
    }

    private void initializeFeed(MountRequest request, PartitionClient client) {
        try {
            long fetchSize = storePartition.highWaterMark() - request.clientHighWaterMark;

            if (fetchSize < 0L) {
                client.sendMessage(new MountResponse(request.reqId, MountResponse.PartitionState.CLIENT_AHEAD), true);
                throw new IllegalStateException("client is ahead of store");
            }

            if (logger.isDebugEnabled()) {
                logger.debug("initial feed size: " + fetchSize);
            }

            MountResponse response = new MountResponse(request.reqId, MountResponse.PartitionState.READY);

            if (fetchSize == 0) {
                // The client is up to date. Send the response immediately. No need to enqueue a feed context.
                client.sendMessage(response, true);
            } else {
                // Must bring the client up to date before mounting the partition
                addFeedContext(request, fetchSize, response, client);
            }
        } catch (StoreException ex) {
            logger.error("failed to initialize feed", ex);
        }
    }

    private void resumeFeed(FeedRequest request, PartitionClient client) throws StoreException {
        long fetchSize = storePartition.highWaterMark() - request.clientHighWaterMark;

        if (fetchSize < minFetchSize) {
            if (fetchSize < 0L) {
                throw new IllegalStateException("client is ahead of store");
            }

            fetchSize = minFetchSize;
        }

        addFeedContext(request, (int) fetchSize, new FeedSuspended(request.reqId), client);
    }

    private void addFeedContext(FeedRequest request, long fetchSize, FeedSuspended suspendMessage, PartitionClient client) throws StoreException {
        FeedContext feedContext =
            new FeedContext(request.reqId, request.clientHighWaterMark, fetchSize, client, suspendMessage);

        if (feedContext.isWritable()) {
            addFeedContext(feedContext);
        } else {
            pauseFeedContext(feedContext);
        }
    }

    private void addFeedContext(FeedContext feedContext) throws StoreException {
        try {
            // Add the new feed context
            if ((storePartition.highWaterMark() - feedContext.highWaterMark()) < realtimeThreshold) {
                nearRealtimeFeedTask.enqueue(feedContext);
            } else {
                logger.debug("added a catch up task: {}", feedContext);
                catchupFeedTask.enqueue(feedContext);
            }
        } finally {
            // Unblock waiting feed threads because the new feed request may be satisfied immediately
            feedSync.unblock();
        }
    }

    private void getTransactionData(TransactionDataRequest request, PartitionClient client) {
        try {
            TransactionKey key = new TransactionKey(partitionId, request.transactionId);
            TransactionData transactionData = transactionFetcher.fetch(key, storePartition);
            client.sendMessage(new TransactionDataResponse(request.reqId, request.transactionId, transactionData.data, transactionData.checksum), true);

        } catch (Throwable ex) {
            if (running.get()) {
                logger.error("failed to get transaction data", ex);
            }
            RpcException exception = new RpcException(ex.toString());
            client.sendMessage(new TransactionDataResponse(request.reqId, request.transactionId, exception), true);
        }
    }

    private static class AppendContext {
        final AppendRequest request;
        final PartitionClient client;

        AppendContext(AppendRequest request, PartitionClient client) {
            this.request = request;
            this.client = client;
        }
    }

    private static class FlushContext extends AppendContext {
        final CompletableFuture<Long> future = new CompletableFuture<>();

        FlushContext() {
            super(null, null);
        }
    }

    private class AppendTask extends QueueConsumerTask<AppendContext> {

        private Locks locks;

        AppendTask() {
            super("Append-P" + partitionId, new RequestQueue<>(new ArrayBlockingQueue<>(100)));
        }

        @Override
        public void init() throws Exception {
            commitHighWaterMark = storePartition.highWaterMark();
            locks = new Locks(lockTableSize, 3, commitHighWaterMark);
        }

        @Override
        protected void process(AppendContext context) throws Exception {
            if (context.request == null) {
                // This is a flush request
                ((FlushContext) context).future.complete(storePartition.flush());

            } else {
                AppendRequest request = context.request;

                Locks.LockRequest lockRequest = Locks.createRequest(request.writeLockRequest, request.readLockRequest, request.appendLockRequest);
                // Begin locking
                while (!locks.begin(lockRequest)) {
                    if (storePartition.numPendingAppends() == 0) {
                        // Retry
                        if (locks.begin(lockRequest)) {
                            break;
                        } else {
                            logger.error("failed to begin lock, nothing to flush: partitionId=" + partitionId + " numActiveLocks=" + locks.numActiveLocks());
                            locks.reset(storePartition.highWaterMark());
                            logger.error("Locks has been reset: partitionId=" + partitionId);
                        }
                    } else {
                        storePartition.flush();
                    }
                }

                long minHighWaterMark = locks.getLockHighWaterMark(lockRequest);
                if (minHighWaterMark > request.clientHighWaterMark) {
                    transactionRejectionMeter.mark(); // measures rate of transaction rejection
                    // Unable to lock since the min high-water mark is bigger than the client's high-water mark.
                    locks.end(lockRequest);
                    context.client.sendMessage(new LockFailure(request.reqId, minHighWaterMark), true);

                } else {
                    try {
                        Timer.Context timerContext = responseLatencyTimer.time();
                        storePartition.append(request.reqId, request.header, request.data, request.checksum, transactionId -> {
                            // The following code is executed when the transaction is resolved.
                            try {
                                if (transactionId >= 0L) {
                                    try {
                                        // Put the entry into the feed cache
                                        feedCachePartition.add(transactionId, request.reqId, request.header);
                                        // Put the entry into the transaction cache
                                        transactionFetcher.cache(
                                            new TransactionKey(partitionId, transactionId),
                                            new TransactionData(request.data, request.checksum)
                                        );
                                        // Commit locks
                                        locks.commit(lockRequest, transactionId);
                                        highWaterMarkMeter.mark(); // measures mark as rate of high water mark change
                                        commitHighWaterMark = transactionId;
                                    } finally {
                                        // Unblock waiting feed threads
                                        feedSync.unblock();
                                    }
                                }
                            } finally {
                                locks.end(lockRequest);
                                timerContext.stop(); // measures latency of response
                            }
                        });
                    } catch (StoreException ex) {
                        // Append failed. End lock.
                        locks.end(lockRequest);
                        throw ex;
                    }
                }
            }
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            if (ex instanceof StorePartitionClosedException) {
                if (running.get()) {
                    logger.warn("exception caught", ex);
                } else {
                    // The store partition is closed. The append task can no longer append transactions.
                    stop();
                }
            } else {
                logger.debug("exception caught", ex);
            }
        }

        /**
         * Registers with the underlying queue a flush request that will eventually be processed by this append task,
         * and the returned CompletableFuture will be completed with its result.
         *
         * @return a {@link CompletableFuture} which will complete with result of the flush operation.
         * @throws InvalidOperationException if the append queue is already closed.
         */
        public CompletableFuture<Long> flush() {
            FlushContext context = new FlushContext();

            if (!enqueue(context)) {
                throw new InvalidOperationException("Append queue is closed.");
            }

            return context.future;
        }

    }

    private class FeedTask extends QueueConsumerTask<FeedContext> {

        private final AtomicLong totalAdded = new AtomicLong(0);
        private final AtomicLong totalRemoved = new AtomicLong(0);
        private FeedData cachedFeedData = null;
        private long highWaterMark = -1L;

        FeedTask(String threadType, BlockingQueue<FeedContext> feedContextQueue) {
            super("Feed-" + threadType + "-P" + partitionId, new RequestQueue<>(feedContextQueue));
        }

        public long totalAdded() {
            return totalAdded.get();
        }

        public long totalRemoved() {
            return totalRemoved.get();
        }

        @Override
        public boolean enqueue(FeedContext feedContext) {
            boolean success = super.enqueue(feedContext);
            if (success) {
                totalAdded.incrementAndGet();
            }
            return success;
        }

        @Override
        public void process(FeedContext feedContext) throws Exception {
            if (feedContext.isActive()) {
                if (feedContext.isWritable()) {
                    try {
                        long nextTransactionId = feedContext.nextTransactionId();

                        if (highWaterMark < nextTransactionId) {
                            long version = feedSync.version();
                            highWaterMark = storePartition.highWaterMark();

                            if (highWaterMark < nextTransactionId) {
                                // Wait for more data or new feed context
                                feedSync.await(version);
                            }
                        }

                        if (highWaterMark >= nextTransactionId) {
                            FeedData feedData;

                            if (cachedFeedData != null && cachedFeedData.transactionId == nextTransactionId) {
                                feedData = cachedFeedData;
                            } else {
                                // Fetch the feed data
                                feedData = feedCachePartition.get(nextTransactionId);
                                if (feedData == null) {
                                    // Prefetch record headers to fill the cache block.
                                    int prefetchSize = FeedCacheBlock.NUM_TRANSACTIONS - (int) (nextTransactionId & FeedCacheBlock.INDEX_MASK);
                                    ArrayList<RecordHeader> recordHeaderList = storePartition.getRecordHeaderList(nextTransactionId, prefetchSize);
                                    if (!recordHeaderList.isEmpty()) {
                                        RecordHeader firstItem = recordHeaderList.get(0);
                                        feedData = new FeedData(firstItem.reqId, firstItem.transactionId, firstItem.header);

                                        feedCachePartition.addAll(recordHeaderList);
                                    }
                                }
                            }

                            if (feedData != null) {
                                // Cache it in a local variable for the next feed context
                                cachedFeedData = feedData;

                                // Send data. Force flushing when we reach the high-water mark.
                                feedContext.send(feedData, highWaterMark == nextTransactionId);
                            }
                        }
                    } finally {
                        if (feedContext.isActive() && feedContext.hasMoreToFetch()) {
                            // Put the feed context back into the queue without incrementing totalAdded
                            super.enqueue(feedContext);
                        } else {
                            totalRemoved.incrementAndGet();
                        }
                    }
                } else {
                    totalRemoved.incrementAndGet();
                    pauseFeedContext(feedContext);
                }
            }
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            if (ex instanceof StorePartitionClosedException) {
                if (running.get()) {
                    logger.error("exception caught", ex);
                } else {
                    // The store partition is closed. The feed task can no longer generate feeds.
                    stop();
                }
            } else {
                logger.error("exception caught", ex);
            }
        }

    }

    private static class FeedSynchronizer {

        private static final long CLOSED = Long.MIN_VALUE;
        private long version = -1L;

        long version() throws PartitionClosedException {
            synchronized (this) {
                if (version != CLOSED) {
                    return version;
                } else {
                    throw new PartitionClosedException("already closed");
                }
            }
        }

        void await(long snapshotVersion) {
            synchronized (this) {
                while (version != CLOSED && version == snapshotVersion) {
                    try {
                        wait();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }
            }
        }

        void unblock() {
            synchronized (this) {
                if (version != CLOSED) {
                    version++;
                }
                notifyAll();
            }
        }

        void close() {
            synchronized (this) {
                version = CLOSED;
                notifyAll();
            }
        }

    }

}
