package com.wepay.waltz.store.internal;

import com.wepay.riff.util.Logging;
import com.wepay.riff.util.RepeatingTask;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.util.BackoffTimer;
import com.wepay.waltz.store.exception.RecoveryAbortedException;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.exception.ReplicaConnectionException;
import com.wepay.waltz.store.exception.ReplicaConnectionFactoryClosedException;
import com.wepay.waltz.store.exception.ReplicaSessionException;
import com.wepay.waltz.store.exception.ReplicaWriterException;
import com.wepay.waltz.store.internal.metadata.ReplicaId;
import com.wepay.zktools.util.Uninterruptibly;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReplicaSession extends LatencyWeightedRoute {

    private static final Logger logger = Logging.getLogger(ReplicaSession.class);
    private static final int CATCH_UP_BATCH_SIZE = 20;

    public final ReplicaId replicaId;

    private final long sessionId;
    private final CompletableFuture<ReplicaConnection> connectionFuture;
    private final ConnectionConfig config;
    private final BackoffTimer backoffTimer;
    private final ReplicaConnectionFactory connectionFactory;
    private final RepeatingTask task;

    final ReplicaReader reader;
    final ReplicaWriter writer;

    private long currentAppendRequestTransactionId = -1L;
    private Iterable<StoreAppendRequest> currentAppendRequestList = null;
    private Voting currentVoting = null;

    private volatile RecoveryManager recoveryManager;
    private volatile StoreSession storeSession;

    public ReplicaSession(final ReplicaId replicaId, long sessionId, final ConnectionConfig config, final ReplicaConnectionFactory connectionFactory) {
        this.replicaId = replicaId;
        this.sessionId = sessionId;
        this.connectionFuture = new CompletableFuture<>();
        this.config = config;
        this.backoffTimer = new BackoffTimer(config.maxRetryInterval);
        this.connectionFactory = connectionFactory;
        this.task = new ReplicaSessionTask();
        this.reader = new ReplicaReader(connectionFuture);
        this.writer = new ReplicaWriter(connectionFuture);
    }

    public void open(final RecoveryManager recoveryManager, final StoreSession storeSession) {
        synchronized (this) {
            this.recoveryManager = recoveryManager;
            this.storeSession = storeSession;
        }
        this.task.start();
    }

    public void close() {
        try {
            if (connectionFuture.isDone()) {
                if (!connectionFuture.isCompletedExceptionally()) {
                    connectionFuture.get().close();
                }
            } else {
                ReplicaConnectionException ex = new ReplicaConnectionException("connect aborted");
                ex.setStackTrace(new StackTraceElement[0]);
                connectionFuture.completeExceptionally(ex);
            }

            backoffTimer.close();

        } catch (Throwable ex) {
            logger.warn("exception when closing session", ex);
        }

        synchronized (this) {
            try {
                task.stop();

                if (recoveryManager != null) {
                    recoveryManager.abort(new RecoveryFailedException("replica session closed: sessionId=" + sessionId));
                    recoveryManager = null;
                }

                if (currentVoting != null) {
                    currentVoting.abstain();

                    currentAppendRequestList = null;
                    currentVoting = null;
                }
            } finally {
                notifyAll();
            }
        }
    }

    public boolean isConnected() {
        return connectionFuture.isDone();
    }

    @Override
    public boolean isClosed() {
        return !task.isRunning();
    }

    public void append(final long transactionId, final Iterable<StoreAppendRequest> requests, final Voting voting) {
        synchronized (this) {
            if (task.isRunning()) {
                if (currentVoting != null) {
                    currentVoting.abstain();
                }
                currentAppendRequestTransactionId = transactionId;
                currentAppendRequestList = requests;
                currentVoting = voting;
            } else {
                voting.abstain();
            }
            notifyAll();
        }
    }

    public long nextTransactionId() throws ReplicaSessionException {
        try {
            return writer.nextTransactionId();

        } catch (ReplicaWriterException ex) {
            throw new ReplicaSessionException("session closed", ex);
        }
    }

    public Record getRecord(long transactionId) throws ReplicaSessionException {
        if (task.isRunning()) {
            if (transactionId < writer.nextTransactionId()) {
                return reader.getRecord(transactionId);
            } else {
                return null;
            }
        } else {
            throw new ReplicaSessionException("session closed");
        }
    }

    public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws ReplicaSessionException {
        if (task.isRunning()) {
            long numRecords = writer.nextTransactionId() - transactionId;
            if (numRecords > 0) {
                int batchSize = numRecords < maxNumRecords ? (int) numRecords : maxNumRecords;
                return reader.getRecordList(transactionId, batchSize);

            } else {
                return new ArrayList<>();
            }
        } else {
            throw new ReplicaSessionException("session closed");
        }
    }

    public RecordHeader getRecordHeader(long transactionId) throws ReplicaSessionException {
        if (task.isRunning()) {
            long nextTransactionId = writer.nextTransactionId();
            if (transactionId < nextTransactionId) {
                return reader.getRecordHeader(transactionId);

            } else {
                return null;
            }
        } else {
            throw new ReplicaSessionException("session closed");
        }
    }

    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws ReplicaSessionException {
        if (task.isRunning()) {
            long numRecords = writer.nextTransactionId() - transactionId;
            if (numRecords > 0) {
                int batchSize = numRecords < maxNumRecords ? (int) numRecords : maxNumRecords;
                return reader.getRecordHeaderList(transactionId, batchSize);

            } else {
                return new ArrayList<>();
            }
        } else {
            throw new ReplicaSessionException("session closed");
        }
    }

    public void awaitRecovery() {
        synchronized (this) {
            while (recoveryManager != null) {
                Uninterruptibly.run(this::wait);
            }
        }
    }

    private ReplicaConnection openConnection() throws ReplicaConnectionException {
        long retryInterval = config.initialRetryInterval;

        while (!connectionFuture.isDone()) {
            try {
                return connectionFactory.get(replicaId.partitionId, sessionId);

            } catch (ReplicaConnectionFactoryClosedException ex) {
                connectionFuture.completeExceptionally(ex);
                break;

            } catch (Exception ex) {
                // Ignore
                if (retryInterval == config.maxRetryInterval) {
                    logger.warn("failed to connect, retrying...", ex);
                }
            }

            retryInterval = backoffTimer.backoff(retryInterval);
        }

        while (true) {
            try {
                return connectionFuture.get();

            } catch (InterruptedException ex) {
                Thread.interrupted();
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof ReplicaConnectionException) {
                    throw (ReplicaConnectionException) cause;
                }
                throw new ReplicaConnectionException("failed to connect", ex);
            }
        }
    }

    private void initialize() throws Exception {
        RecoveryManager recoveryMgr = recoveryManager; // for safety

        if (recoveryMgr != null) {
            // Open a replica connection
            ReplicaConnection connection = openConnection();

            // Now make the connection available to replica reader and writer
            if (!connectionFuture.complete(connection)) {
                connection.close();
            }

            // Start recovery
            long nextTransactionId = recoveryMgr.start(replicaId, connection) + 1;

            writer.open(nextTransactionId);

            // End recovery
            recoveryMgr.end(replicaId);

            synchronized (this) {
                // Recovery completed
                recoveryManager = null;
                notifyAll();
            }

        } else {
            logger.error("recovery manager is null");
        }
    }

    @SuppressFBWarnings(value = "WA_NOT_IN_LOOP", justification = "the next call decides what to do")
    private boolean tryExecuteCurrentAppendRequests(long nextTransactionId) throws Exception {
        long requestTransactionId;
        Iterable<StoreAppendRequest> requests;
        Voting voting;

        synchronized (this) {
            requestTransactionId = currentAppendRequestTransactionId;

            if (nextTransactionId == requestTransactionId) {
                // We caught up and have append requests to execute.
                // Append the transactions after exiting this sync block.
                requests = currentAppendRequestList;
                voting = currentVoting;
                currentAppendRequestList = null;
                currentVoting = null;

            } else if (nextTransactionId > requestTransactionId) {
                // We caught up and have nothing to execute, just wait for new requests
                if (task.isRunning()) {
                    wait();
                }
                return true;

            } else {
                // We are falling behind. Failed to process the current requests
                return false;
            }
        }

        // Append the transactions
        try {
            writer.append(requestTransactionId, requests);
            voting.vote();

        } catch (ReplicaWriterException ex) {
            // Write failed. We don't know if the transaction is written to the storage.
            voting.abstain();
            throw ex;
        }
        // We successfully executed the current append requests.
        return true;
    }

    private void catchUp(long nextTransactionId) throws Exception {
        long numRecords;

        synchronized (this) {
            numRecords = currentAppendRequestTransactionId - nextTransactionId;
        }

        while (numRecords > 0 && task.isRunning()) {
            // Get a batch of committed transactions
            int batchSize = numRecords < CATCH_UP_BATCH_SIZE ? (int) numRecords : CATCH_UP_BATCH_SIZE;
            ArrayList<Record> batch = storeSession.getRecordList(nextTransactionId, batchSize);
            // Write the batch
            writer.append(batch);
            nextTransactionId += batch.size();
            synchronized (this) {
                numRecords = currentAppendRequestTransactionId - nextTransactionId;
            }
        }
    }

    private class ReplicaSessionTask extends RepeatingTask {

        ReplicaSessionTask() {
            super("ReplicaSession-" + replicaId.partitionId + "-" + sessionId);
        }

        @Override
        protected void init() throws Exception {
            initialize();
        }

        @Override
        protected void task() throws Exception {
            long nextTransactionId = writer.nextTransactionId();

            // Try to execute the current append requests
            if (!tryExecuteCurrentAppendRequests(nextTransactionId)) {
                // Failed to execute the current append requests. We are falling behind.
                catchUp(nextTransactionId);
            }
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            // We close the replica session whenever an exception is thrown.
            if (ex instanceof RecoveryAbortedException) {
                logger.warn("closing a replica session task: aborted");
            } else {
                logger.warn("closing a replica session task", ex);
            }
            close();
        }
    }

}
