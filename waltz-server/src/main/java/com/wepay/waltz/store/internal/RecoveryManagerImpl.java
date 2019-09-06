package com.wepay.waltz.store.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.exception.RecoveryAbortedException;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.internal.metadata.PartitionMetadata;
import com.wepay.waltz.store.internal.metadata.PartitionMetadataSerializer;
import com.wepay.waltz.store.internal.metadata.ReplicaId;
import com.wepay.waltz.store.internal.metadata.ReplicaState;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class implements the {@link RecoveryManager}.
 */
public class RecoveryManagerImpl implements RecoveryManager {

    private static final Logger logger = Logging.getLogger(RecoveryManagerImpl.class);

    private static final int MAX_BATCH_SIZE = 20;
    private static final Comparator<ReplicaRecoveryState> HIGH_WATER_MARK_DESC = (o1, o2) -> Long.compare(o2.highWaterMark, o1.highWaterMark);

    private final int generation;
    private final long sessionId;

    private final int quorum;
    private final HashMap<ReplicaId, ReplicaRecoveryState> replicaRecoveryStates = new HashMap<>();
    private final ArrayList<ReplicaSession> replicaSessions = new ArrayList<>();

    private final ZooKeeperClient zkClient;
    private final ZNode znode;

    private final Object lock = new Object();
    private final AtomicReference<RecoveryAbortedException> recoveryAborted = new AtomicReference<>(null);
    private final AtomicReference<Long> resolvedHighWaterMark = new AtomicReference<>(null);
    private long minVotingHighWaterMark = Long.MAX_VALUE;

    private final Set<ReplicaId> recoveredReplicas = Collections.synchronizedSet(new HashSet<>());
    private final Set<ReplicaId> cleanReplicas = Collections.synchronizedSet(new HashSet<>());
    private PartitionMetadata partitionMetadata;
    private int partitionMetadataVersion;

    private boolean started = false;

    /**
     * Class constructor.
     * @param generation The generation number.
     * @param sessionId The session Id.
     * @param quorum The Quorum required.
     * @param zkClient The ZooKeeperClient used for the Waltz Cluster.
     * @param znode Path to the znode.
     */
    public RecoveryManagerImpl(int generation, long sessionId, int quorum, ZooKeeperClient zkClient, ZNode znode) {
        this.generation = generation;
        this.sessionId = sessionId;
        this.quorum = quorum;
        this.zkClient = zkClient;
        this.znode = znode;
    }

    @Override
    public void start(ArrayList<ReplicaSession> replicaSessions) {
        Throwable exception = null;

        logger.debug("recovery starting");

        synchronized (lock) {
            try {
                if (started) {
                    throw new RecoveryFailedException("already started");
                }

                this.replicaSessions.addAll(replicaSessions);

                loadPartitionMetadata();

            } catch (Throwable ex) {
                exception = ex;

            } finally {
                started = true;
                lock.notifyAll();
            }
        }

        if (exception != null) {
            abort(exception);
        }
    }

    @Override
    public void abort(Throwable cause) {
        if (recoveryAborted.compareAndSet(null, new RecoveryAbortedException(cause))) {
            logger.warn("aborting", cause);

            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    private RecoveryFailedException fail(Throwable cause) {
        abort(cause);

        if (cause instanceof RecoveryFailedException) {
            return (RecoveryFailedException) cause;
        } else {
            return new RecoveryFailedException("failed to recover", cause);
        }
    }

    @Override
    public long start(ReplicaId replicaId, ReplicaConnection connection) throws RecoveryFailedException {
        awaitStart();

        logger.debug("recovery starting: {}", replicaId);

        try {
            ReplicaState replicaState;
            boolean isNew = false;

            synchronized (lock) {
                replicaState = partitionMetadata.replicaStates.get(replicaId);
                if (replicaState == null) {
                    if (partitionMetadata.replicaStates.size() == 0) {
                        replicaState = new ReplicaState(replicaId, -1L, ReplicaState.UNRESOLVED);
                    } else {
                        isNew = true;
                        replicaState = new ReplicaState(replicaId, -1L, -1L);
                    }
                }
            }

            long maxTransactionId;
            boolean voting;

            SessionInfo lastSessionInfo = connection.lastSessionInfo();

            if (lastSessionInfo.sessionId < replicaState.sessionId) {
                // The replica state in ZK and the storage are inconsistent.
                // The storage might have been restored to some old state by ops.
                // This replica should not cast a vote.
                logger.warn("Database and metadata in ZK are inconsistent, not voting");

                // Truncate the transaction log to the low-water mark and bring the log to a clean state.
                // If the replica was falling behind, the following truncation may not remove any row.
                // If it is the case, the max transaction id of the replica will be smaller than the low-water mark.
                maxTransactionId = connection.truncate(lastSessionInfo.lowWaterMark);

                // This replica is now clean but too behind to participate in voting.
                voting = false;

            } else if (isNew) {
                // Truncate the transaction log to the low-water mark and bring the log to a clean state.
                // If the replica was falling behind, the following truncation may not remove any row.
                // If it is the case, the max transaction id of the replica will be smaller than the low-water mark.
                maxTransactionId = connection.truncate(lastSessionInfo.lowWaterMark);

                // This replica is now clean but too behind to participate in voting.
                voting = false;

            } else {
                if (replicaState.closingHighWaterMark != ReplicaState.UNRESOLVED) {
                    // This replica may have dirty records. Truncate them.
                    maxTransactionId = connection.truncate(replicaState.closingHighWaterMark);

                    // This replica is now clean but too behind to participate in voting.
                    voting = false;

                } else {
                    // This replica may not be clean.
                    maxTransactionId = connection.getMaxTransactionId();

                    // If the replica is falling far behind, the max transaction id is smaller
                    // than the session's low-water mark.
                    if (maxTransactionId < lastSessionInfo.lowWaterMark) {
                        // This replica is clean, but too behind to participate in voting.
                        voting = false;

                    } else {
                        // This replica may not be clean. We must resolve the high-water mark by voting.
                        voting = true;
                    }
                }
            }

            resolve(replicaState.replicaId, connection, voting, maxTransactionId);

            return resolvedHighWaterMark.get();

        } catch (Throwable ex) {
            throw fail(ex);
        }
    }

    /**
     * This method handles recovery completion.
     * @param replicaId The replica Id.
     * @throws RecoveryFailedException thrown if the recovery fails.
     */
    public void end(ReplicaId replicaId) throws RecoveryFailedException {
        recoveredReplicas.add(replicaId);
        cleanReplicas.add(replicaId);

        if (recoveredReplicas.size() >= quorum) {
            // Update metadata in ZK
            updateReplicaStates();

        } else {
            awaitCompletion();
        }

        logger.debug("recovery complete : {}", replicaId);
    }

    private void resolve(ReplicaId replicaId, ReplicaConnection connection, boolean voting, long highWaterMark) throws RecoveryFailedException {
        Set<ReplicaId> lastReplicaIds;
        int lastQuorum;

        synchronized (lock) {
            if (voting) {
                if (minVotingHighWaterMark > highWaterMark) {
                    minVotingHighWaterMark = highWaterMark;
                }
            } else {
                // A non-voting replica should always be clean.
                cleanReplicas.add(replicaId);
            }

            lastReplicaIds = partitionMetadata.replicaStates.keySet();
            lastQuorum = partitionMetadata.replicaStates.size() / 2 + 1;

            lock.notifyAll();
        }

        // If all the replicas are new, set resolved high-water mark to -1
        if (lastReplicaIds.isEmpty()) {
            // Don't return yet because we need to set low-water mark of this session
            resolvedHighWaterMark.compareAndSet(null, -1L);
        }

        boolean lowWaterMarkSet = false;
        boolean isNew = !lastReplicaIds.contains(replicaId);
        ReplicaRecoveryState replicaRecoveryState = new ReplicaRecoveryState(replicaId, connection, highWaterMark, isNew);

        try {
            while (true) {
                checkAbort();

                if (resolvedHighWaterMark.get() != null) {
                    if (!lowWaterMarkSet) {
                        // If the replica has dirty records, truncate them.
                        if (replicaRecoveryState.highWaterMark >= resolvedHighWaterMark.get()) {
                            replicaRecoveryState = replicaRecoveryState.update(connection.truncate(resolvedHighWaterMark.get()));
                        }

                        // Set the low-water mark of this session.
                        connection.setLowWaterMark(resolvedHighWaterMark.get());
                        lowWaterMarkSet = true;
                    }

                    if (replicaRecoveryState.highWaterMark >= resolvedHighWaterMark.get()) {
                        // The replica has fully caught up.
                        break;
                    }
                }

                ReplicaRecoveryState usher = null;
                int replicaRecoveryStatesSize;

                synchronized (lock) {
                    replicaRecoveryStates.put(replicaId, replicaRecoveryState);
                    replicaRecoveryStatesSize = replicaRecoveryStates.size();

                    // Decide resolved high-water mark regardless of new replicas.
                    ReplicaRecoveryState[] states = replicaRecoveryStates.values().stream()
                            .filter((state -> !state.isNew))
                            .toArray(ReplicaRecoveryState[]::new);

                    // Sort the replica recovery states by high-water marks in descending order.
                    Arrays.sort(states, HIGH_WATER_MARK_DESC);

                    // Calculate number of absent replicas regardless of new replicas.
                    // Removed replicas is not treated as absent.
                    int numAbsentReplicas = (int) (lastReplicaIds.size()
                            - replicaRecoveryStates.values().stream().filter(replica -> !replica.isNew).count());

                    boolean undecidable = false;
                    int supports = 0;

                    for (int i = 0; i < states.length; i++) {
                        ReplicaRecoveryState state = states[i];

                        if (state.highWaterMark > replicaRecoveryState.highWaterMark) {
                            usher = state;

                        } else if (state.highWaterMark < replicaRecoveryState.highWaterMark) {
                            break;
                        }

                        if (resolvedHighWaterMark.get() == null) {
                            // We consider all members with high-water marks equal to or greater than minVotingHighWaterMark.
                            if (minVotingHighWaterMark <= replicaRecoveryState.highWaterMark) {
                                supports++;
                                if (i == states.length - 1 || state.highWaterMark > states[i + 1].highWaterMark) {
                                    if (supports < lastQuorum && supports + numAbsentReplicas >= lastQuorum) {
                                        // This high-water mark has not established the quorum, yet.
                                        // However, absent replicas may change it later. Thus, undecidable.
                                        undecidable = true;
                                    }

                                    if (!undecidable && supports >= lastQuorum) {
                                        resolvedHighWaterMark.compareAndSet(null, state.highWaterMark);
                                    }
                                }
                            }
                        }
                    }

                    lock.notifyAll();
                }

                if (usher == null) {
                    synchronized (lock) {
                        if (resolvedHighWaterMark.get() == null && recoveryAborted.get() == null) {
                            if (replicaRecoveryStatesSize != replicaRecoveryStates.size()) {
                                logger.debug("replica recovery states changed, recalculating usher... : {}", replicaId);
                            } else {
                                // This replica has the current highest high-water mark. There is nothing to catch up for now.
                                try {
                                    logger.debug("nothing to catch up, waiting... : {}", replicaId);
                                    lock.wait();
                                } catch (InterruptedException ex) {
                                    Thread.interrupted();
                                }
                            }
                        }
                    }
                } else {
                    // Follow the usher to catch up.
                    logger.debug("catching up... : {}", replicaId);
                    long catchUpHighWaterMark = catchUpWithUsher(replicaRecoveryState, usher, connection);

                    replicaRecoveryState = replicaRecoveryState.update(catchUpHighWaterMark);
                }
            }

            // The high-water mark is resolved, and the replica has fully caught up.
            // If the replica has dirty records, truncate them.
            if (replicaRecoveryState.highWaterMark > resolvedHighWaterMark.get()) {
                connection.truncate(resolvedHighWaterMark.get());
            }

            logger.debug("resolved : {}", replicaId);

        } catch (Throwable ex) {
            throw fail(ex);
        }
    }

    /**
     * Replica follows the usher to catch up, and return its high-water mark after complete.
     * @param replicaRecoveryState replica recovery state
     * @param usher the usher to catch up with
     * @param connection replica connection
     * @return currentHighWaterMark
     * @throws StorageRpcException
     */
    private long catchUpWithUsher(ReplicaRecoveryState replicaRecoveryState, ReplicaRecoveryState usher, ReplicaConnection connection) throws StorageRpcException {
        long currentHighWaterMark = replicaRecoveryState.highWaterMark;
        long targetHighWaterMark = resolvedHighWaterMark.get() != null ? resolvedHighWaterMark.get() : usher.highWaterMark;

        while (currentHighWaterMark < targetHighWaterMark) {
            int batchSize = (int) Math.min((targetHighWaterMark - currentHighWaterMark), (long) MAX_BATCH_SIZE);
            ArrayList<Record> records = usher.connection.getRecordList(currentHighWaterMark + 1, batchSize);
            connection.appendRecords(records);
            currentHighWaterMark += records.size();
            targetHighWaterMark = resolvedHighWaterMark.get() != null ? resolvedHighWaterMark.get() : targetHighWaterMark;
        }

        return currentHighWaterMark;
    }

    @Override
    public long highWaterMark() throws RecoveryFailedException {
        awaitCompletion();

        if (resolvedHighWaterMark.get() != null) {
            return resolvedHighWaterMark.get();
        } else {
            throw new RecoveryFailedException("completed normally, but high-water mark is not resolved");
        }
    }

    private void awaitStart() throws RecoveryFailedException {
        synchronized (lock) {
            // Wait for start
            while (!started && recoveryAborted.get() == null) {
                try {
                    lock.wait();

                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        }

        checkAbort();
    }

    private void awaitCompletion() throws RecoveryFailedException {
        synchronized (lock) {
            // Wait for completion
            while (recoveredReplicas.size() < quorum && recoveryAborted.get() == null) {
                try {
                    lock.wait();

                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        }

        checkAbort();
    }

    protected void updateReplicaStates() throws RecoveryFailedException {
        synchronized (lock) {
            lock.notifyAll();

            checkAbort();

            while (partitionMetadata.sessionId == sessionId) {
                HashMap<ReplicaId, ReplicaState> newReplicaStates = new HashMap<>();

                // The commit high-water mark is used for recovery by all active replicas in the previous session.
                for (ReplicaSession replicaSession : replicaSessions) {
                    ReplicaId replicaId = replicaSession.replicaId;

                    if (cleanReplicas.contains(replicaId)) {
                        // This replica is clean. Set a new replica state with the new session id and unset the recovery high-water mark.
                        newReplicaStates.put(replicaId, new ReplicaState(replicaId, sessionId, ReplicaState.UNRESOLVED));

                    } else {
                        ReplicaState replicaState = partitionMetadata.replicaStates.get(replicaId);

                        if (replicaState == null) {
                            // This must be a new replica. Use a dummy session id.
                            newReplicaStates.put(replicaId, new ReplicaState(replicaId, -1L, resolvedHighWaterMark.get()));

                        } else if (replicaState.sessionId < sessionId) {
                            if (replicaState.closingHighWaterMark == ReplicaState.UNRESOLVED) {
                                // This was an active replica and has not been recovered yet. Until it is fully recovered,
                                // it cannot participate in the quorum voting in the next recovery process.
                                // Transactions in this replicas is valid up to the resolved closing high-water mark.
                                // Set the closing high-water mark. Note that session id is not changed.
                                newReplicaStates.put(replicaId, new ReplicaState(replicaId, replicaState.sessionId, resolvedHighWaterMark.get()));

                            } else {
                                // This was an inactive replica. Do not change the replica state.
                                newReplicaStates.put(replicaId, replicaState);
                            }
                        } else {
                            // This replica has already been recovered. Do not change the replica state. (Is this possible?)
                            newReplicaStates.put(replicaId, replicaState);
                        }
                    }
                }
                // At this point, each replica is either recovered or given the closingHighWaterMark.

                try {
                    PartitionMetadata newPartitionMetadata = new PartitionMetadata(generation, sessionId, newReplicaStates);

                    // Write and reload only when changed.
                    if (!newPartitionMetadata.equals(partitionMetadata)) {
                        zkClient.setData(znode, newPartitionMetadata, PartitionMetadataSerializer.INSTANCE, partitionMetadataVersion);
                        loadPartitionMetadata();
                    }

                    return;

                } catch (RecoveryFailedException ex) {
                    throw ex;

                } catch (Exception ex) {
                    // Ignore and retry.
                    logger.debug("failed to update ReplicaStates", ex);
                }

                // Check if the recovery is aborted already.
                checkAbort();

                // Reload the partition metadata. This will fail if the store session was invalidated.
                loadPartitionMetadata();
            }
        }
    }

    private void loadPartitionMetadata() throws RecoveryFailedException {
        NodeData<PartitionMetadata> nodeData;

        try {
            nodeData = zkClient.getData(znode, PartitionMetadataSerializer.INSTANCE);
        } catch (Exception ex) {
            throw fail(new RecoveryFailedException("failed to get partition metadata", ex));
        }

        if (nodeData.value == null) {
            throw fail(new RecoveryFailedException("partition metadata node missing"));
        }

        if (nodeData.value.sessionId != sessionId) {
            throw fail(new RecoveryFailedException("session id mismatch"));
        }

        partitionMetadata = nodeData.value;
        partitionMetadataVersion = nodeData.stat.getVersion();
    }

    private void checkAbort() throws RecoveryAbortedException {
        if (recoveryAborted.get() != null) {
            throw recoveryAborted.get();
        }
    }

    private static class ReplicaRecoveryState {

        final ReplicaId replicaId;
        final ReplicaConnection connection;
        final long highWaterMark;
        final boolean isNew;

        ReplicaRecoveryState(ReplicaId replicaId, ReplicaConnection connection, long highWaterMark, boolean isNew) {
            this.replicaId = replicaId;
            this.connection = connection;
            this.highWaterMark = highWaterMark;
            this.isNew = isNew;
        }

        ReplicaRecoveryState update(long newHighWaterMark) {
            return new ReplicaRecoveryState(replicaId, connection, newHighWaterMark, isNew);
        }

    }

}
