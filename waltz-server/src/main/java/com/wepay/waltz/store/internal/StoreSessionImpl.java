package com.wepay.waltz.store.internal;

import com.wepay.riff.util.Logging;
import com.wepay.riff.util.RepeatingTask;
import com.wepay.riff.util.RequestQueue;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.exception.ReplicaSessionException;
import com.wepay.waltz.store.exception.SessionClosedException;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Implements {@link StoreSession}.
 */
public class StoreSessionImpl implements StoreSession {

    private static final Logger logger = Logging.getLogger(StoreSessionImpl.class);
    private static final int BATCH_SIZE = 100;

    public final int generation;
    public final int partitionId;
    public final long sessionId;

    private final int numReplicas;
    private final int quorum;
    private final ArrayList<ReplicaSession> replicaSessions;
    private final StoreSessionTask task;
    private final RequestQueue<StoreAppendRequest> requestQueue = new RequestQueue<>(new ArrayBlockingQueue<>(BATCH_SIZE * 2));
    private final Object requestQueueProcessingLock = new Object();

    private LatencyWeightedRouter<ReplicaSession> router = null;

    private int numPending = 0;
    private List<StoreAppendRequest> pendingAppendRequests = null;

    private volatile RecoveryManager recoveryManager;
    private volatile long lowWaterMark = Long.MIN_VALUE;
    private volatile long nextTransactionId;
    private volatile boolean running = true;

    /**
     * Class constructor.
     * @param partitionId The partition Id.
     * @param generation The generation number.
     * @param sessionId The session Id.
     * @param replicaSessions List of {@link ReplicaSession}s.
     * @param zkClient The ZooKeeperClient used in Waltz cluster.
     * @param znode Path to the znode.
     */
    public StoreSessionImpl(
        final int partitionId,
        final int generation,
        final long sessionId,
        final ArrayList<ReplicaSession> replicaSessions,
        final ZooKeeperClient zkClient,
        final ZNode znode
    ) {
        this.generation = generation;
        this.partitionId = partitionId;
        this.sessionId = sessionId;
        this.numReplicas = replicaSessions.size();
        this.quorum = this.numReplicas / 2 + 1;
        this.replicaSessions = replicaSessions;

        this.recoveryManager = new RecoveryManagerImpl(generation, sessionId, quorum, zkClient, znode);
        this.task = new StoreSessionTask();
    }

    /**
     * Opens the store session.
     * @throws RecoveryFailedException thrown if the recovery fails.
     * @throws StoreException thrown is fail to read the store metadata.
     */
    public void open() throws RecoveryFailedException, StoreException {
        if (quorum < 1) {
            throw new StoreException("not enough replicas");
        }

        try {
            recoveryManager.start(replicaSessions);

            for (ReplicaSession replicaSession : replicaSessions) {
                replicaSession.open(recoveryManager, this);
            }

            // Get the resolved high-water mark. This call may be blocked.
            // This is the low-water mark of this session.
            lowWaterMark = recoveryManager.highWaterMark();
            nextTransactionId = lowWaterMark + 1;

            // Add replica sessions to the router to make them available for get-RPCs
            router = new LatencyWeightedRouter<>(replicaSessions);

        } catch (RecoveryFailedException ex) {
            close();
            throw ex;
        } catch (Throwable ex) {
            close();
            throw new StoreException("failed to recover", ex);
        } finally {
            recoveryManager = null;
        }

        task.start();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (running) {
                running = false;
                try {
                    task.stop();
                    requestQueue.close();

                    if (recoveryManager != null) {
                        recoveryManager.abort(new RecoveryFailedException("store session closed"));
                    }

                    for (ReplicaSession replicaSession : replicaSessions) {
                        replicaSession.close();
                    }
                } finally {
                    // wake up a flushing thread
                    notifyAll();
                }
             }
        }
    }

    @Override
    public int generation() {
        return generation;
    }

    @Override
    public long highWaterMark() {
        return nextTransactionId - 1;
    }

    @Override
    public long lowWaterMark() {
        return lowWaterMark;
    }

    @Override
    public boolean isWritable() {
        return running;
    }

    @Override
    public void append(StoreAppendRequest request) throws SessionClosedException, GenerationMismatchException {
        synchronized (this) {
            if (running) {
                if (request.reqId.generation() != generation) {
                    throw new GenerationMismatchException();
                }

                if (requestQueue.enqueue(request)) {
                    numPending++;
                }

            } else {
                throw new SessionClosedException();
            }
        }
    }

    @Override
    public int numPendingAppends() {
        synchronized (this) {
            return numPending;
        }
    }

    @Override
    public void resolveAllAppendRequests(long highWaterMark) {
        synchronized (requestQueueProcessingLock) {
            synchronized (this) {
                if (running) {
                    logger.error("store session still running");
                }

                resolveAppendRequests(highWaterMark);

                pendingAppendRequests = requestQueue.toList();
                resolveAppendRequests(highWaterMark);

                // wake up a flushing thread
                notifyAll();
            }
        }
    }

    // Resolves requests as success if transaction id <= high-water mark, otherwise as failure
    private void resolveAppendRequests(long highWaterMark) {
        synchronized (this) {
            if (pendingAppendRequests != null) {
                for (StoreAppendRequest request : pendingAppendRequests) {
                    if (!request.isCommitted()) {
                        if (nextTransactionId <= highWaterMark) {
                            request.commit(nextTransactionId++);
                        } else {
                            request.commit(-1L);
                        }
                    }
                }

                numPending -= pendingAppendRequests.size();
                pendingAppendRequests = null;

                if (numPending == 0) {
                    // wake up a flushing thread
                    notifyAll();
                }
            }
        }
    }

    @Override
    public long flush() throws SessionClosedException {
        synchronized (this) {
            while (numPending > 0) {
                if (running) {
                    try {
                        wait();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                } else {
                    throw new SessionClosedException();
                }
            }
            return highWaterMark();
        }
    }

    @Override
    public Record getRecord(long transactionId) throws SessionClosedException {
        if (transactionId < nextTransactionId) {
            return getValue(transactionId, replicaSession -> replicaSession.getRecord(transactionId));
        } else {
            return null;
        }
    }

    @Override
    public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws SessionClosedException {
        long numRecords = nextTransactionId - transactionId;
        if (numRecords > 0) {
            int batchSize = numRecords < maxNumRecords ? (int) numRecords : maxNumRecords;
            return getValue(transactionId, replicaSession -> replicaSession.getRecordList(transactionId, batchSize));

        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public RecordHeader getRecordHeader(long transactionId) throws SessionClosedException {
        if (transactionId < nextTransactionId) {
            return getValue(transactionId, replicaSession -> replicaSession.getRecordHeader(transactionId));

        } else {
            return null;
        }
    }

    @Override
    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws SessionClosedException {
        long numRecords = nextTransactionId - transactionId;
        if (numRecords > 0) {
            int batchSize = numRecords < maxNumRecords ? (int) numRecords : maxNumRecords;
            return getValue(transactionId, replicaSession -> replicaSession.getRecordHeaderList(transactionId, batchSize));

        } else {
            return new ArrayList<>();
        }
    }

    private <T> T getValue(long transactionId, ValueGetter<T> callable) throws SessionClosedException {
        if (running) {
            try {
                while (true) {
                    ReplicaSession replicaSession = router.getRoute();

                    if (replicaSession == null) {
                        // No replica session found.
                        close();
                        break;
                    }

                    if (transactionId < replicaSession.nextTransactionId()) {
                        long startTime = System.currentTimeMillis();

                        T value = callable.getValue(replicaSession);

                        replicaSession.updateExpectedLatency(System.currentTimeMillis() - startTime);

                        return value;

                    } else {
                        // The replica is lagging. Use the max read latency.
                        replicaSession.updateExpectedLatency(LatencyWeightedRouter.MAX_LATENCY);
                    }
                }

            } catch (ReplicaSessionException ex) {
                close();
            }
        }
        throw new SessionClosedException();
    }

    private void doAppend() {
        synchronized (requestQueueProcessingLock) {
            List<StoreAppendRequest> batch = requestQueue.dequeue(BATCH_SIZE);

            if (batch != null && batch.size() > 0) {
                synchronized (this) {
                    // Bookkeeping the pending requests
                    if (pendingAppendRequests == null) {
                        pendingAppendRequests = batch;
                    } else {
                        throw new IllegalStateException("there are pending append requests already");
                    }
                }

                // Dynamically adjust number of voters. Only replica that successfully connected will be treated as voter.
                int numVoters = 0;
                for (ReplicaSession replicaSession : replicaSessions) {
                    numVoters = replicaSession.isConnected() ? numVoters + 1 : numVoters;
                }
                Voting voting = new Voting(quorum, numVoters);

                for (ReplicaSession replicaSession : replicaSessions) {
                    replicaSession.append(nextTransactionId, batch, voting);
                }

                if (voting.await()) {
                    // Transactions succeeded. commit all transactions.
                    resolveAppendRequests(Long.MAX_VALUE);

                    if (voting.hasAbstention()) {
                        // Some replica failed. We must start a new session.
                        close();
                    }
                } else {
                    // Transactions failed. Close the session so that no one can write a transaction using this session.
                    // Transactions will be resolved when the next session is started.
                    close();
                }
            }
        }

    }

    private class StoreSessionTask extends RepeatingTask {

        StoreSessionTask() {
            super("StoreSession-" + partitionId + "-" + sessionId);
        }

        @Override
        protected void task() throws InterruptedException {
            doAppend();
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            logger.error("exception caught", ex);
            close();
        }

    }

    private interface ValueGetter<T> {

        T getValue(ReplicaSession replicaSession) throws ReplicaSessionException;

    }

}
