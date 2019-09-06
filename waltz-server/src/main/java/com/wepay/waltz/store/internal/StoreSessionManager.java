package com.wepay.waltz.store.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.util.BackoffTimer;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.store.exception.StoreSessionManagerException;
import com.wepay.waltz.store.internal.metadata.PartitionMetadata;
import com.wepay.waltz.store.internal.metadata.PartitionMetadataSerializer;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hanldes {@link StoreSession}s.
 */
public class StoreSessionManager {

    private static final Logger logger = Logging.getLogger(StoreSessionManager.class);
    private static final long MIN_METADATA_BACKOFF = 100;
    private static final long MAX_METADATA_BACKOFF = 1000;

    public final int partitionId;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ReplicaSessionManager replicaSessionManager;
    private final BackoffTimer backoffTimer = new BackoffTimer(MAX_METADATA_BACKOFF);
    private final Object sessionAssignLock = new Object();

    private ZooKeeperClient zkClient;
    private ZNode znode;

    private final AtomicInteger generation;
    private volatile boolean healthy = true;
    private volatile StoreSession currentSession;

    /**
     * Class constructor.
     * @param partitionId The partition Id.
     * @param generation The generation number.
     * @param replicaSessionManager The {@link ReplicaSessionManager}.
     * @param zkClient The Zoo Keeper Client used in the Waltz cluster.
     * @param znode Path to the znode.
     */
    public StoreSessionManager(
        final int partitionId,
        final int generation,
        final ReplicaSessionManager replicaSessionManager,
        final ZooKeeperClient zkClient,
        final ZNode znode
    ) {
        this.partitionId = partitionId;
        this.generation = new AtomicInteger(generation);
        this.zkClient = zkClient;
        this.znode = znode;
        this.replicaSessionManager = replicaSessionManager;
    }

    /**
     * Closes the store session manager
     */
    public void close() {
        if (running.compareAndSet(true, false)) {
            synchronized (sessionAssignLock) {
                if (currentSession != null) {
                    // Close the current session
                    currentSession.close();
                    currentSession = null;
                }
            }
            backoffTimer.close();
        }
    }

    /**
     * Set the generation number
     * @param generation the generation number
     */
    public void generation(int generation) {
        int current = this.generation.get();
        while (current < generation) {
            if (this.generation.compareAndSet(current, generation)) {
                // We will create a new session later
                break;
            }
            current = this.generation.get();
        }
    }

    /**
     * Returns the generation number
     * @return the generation number
     */
    public int generation() {
        return generation.get();
    }

    /**
     * Returns true if the store session manager is healthy, i.e., there is no recovery error currently.
     * @return
     */
    public boolean isHealthy() {
        return healthy;
    }

    /**
     * Returns a store session.
     * @return a store session
     * @throws RecoveryFailedException
     * @throws StoreSessionManagerException
     */
    public StoreSession getStoreSession() throws RecoveryFailedException, GenerationMismatchException, StoreSessionManagerException {
        synchronized (this) {
            while (running.get()) {
                StoreSession session = currentSession;

                if (session != null) {
                    if (session.generation() == generation.get() && session.isWritable()) {
                        return session;
                    }
                }

                createSession(generation.get());
            }
            throw new StoreSessionManagerException();
        }
    }

    /**
     * Returns the number of pending append requests
     * @return the number of pending append requests
     */
    public int numPendingAppends() {
        StoreSession session = currentSession;
        return session != null ? session.numPendingAppends() : 0;
    }

    /**
     * Returns a store session if the current generation is equal to the specified generation number.
     * @param generation
     * @return a store session
     * @throws RecoveryFailedException
     * @throws GenerationMismatchException
     * @throws StoreSessionManagerException
     */
    public StoreSession getStoreSession(int generation) throws RecoveryFailedException, GenerationMismatchException, StoreSessionManagerException {
        synchronized (this) {
            if (this.generation.get() != generation) {
                throw new GenerationMismatchException();
            }

            StoreSession session = getStoreSession();

            if (session.generation() > generation) {
                throw new GenerationMismatchException();
            }

            return session;
        }
    }

    private void createSession(int generation) throws RecoveryFailedException, GenerationMismatchException, StoreSessionManagerException {
        if (currentSession != null) {
            // Close the current session
            currentSession.close();
        }

        StoreSessionImpl session = null;

        while (running.get() && session == null) {
            try {
                PartitionMetadata partitionMetadata = updatePartitionMetadata(generation);

                final long sessionId = partitionMetadata.sessionId;

                // Create a new session
                ArrayList<ReplicaSession> replicaSessions = replicaSessionManager.getReplicaSessions(partitionId, sessionId);
                session = new StoreSessionImpl(partitionId, generation, sessionId, replicaSessions, zkClient, znode);
                session.open();
                healthy = true;

                // Resolve pending transactions in the last session
                if (currentSession != null) {
                    currentSession.resolveAllAppendRequests(session.highWaterMark());
                    // We have resolved all pending append requests. Nullify the current session.
                    synchronized (sessionAssignLock) {
                        currentSession = null;
                    }
                }

           } catch (GenerationMismatchException ex) {
                if (session != null) {
                    session.close();
                }
                throw ex;
            } catch (RecoveryFailedException ex) {
                healthy = false;
                if (session != null) {
                    session.close();
                }
                throw ex;

            } catch (Exception ex) {
                if (session != null) {
                    session.close();
                }
                logger.error("failed to create a session", ex);
                throw new StoreSessionManagerException();
            }
        }

        synchronized (sessionAssignLock) {
            if (running.get()) {
                currentSession = session;
            }
        }
    }

    private PartitionMetadata updatePartitionMetadata(int generation) throws StoreException, ZooKeeperClientException, KeeperException {
        PartitionMetadataSerializer serializer = PartitionMetadataSerializer.INSTANCE;
        long retryInterval = MIN_METADATA_BACKOFF;

        while (true) {
            NodeData<PartitionMetadata> nodeData;
            try {
                nodeData = zkClient.getData(znode, serializer);

            } catch (KeeperException.NoNodeException ex) {
                zkClient.create(znode, PartitionMetadata.EMPTY, serializer, CreateMode.PERSISTENT);
                nodeData = zkClient.getData(znode, serializer);
            }

            PartitionMetadata oldPartitionMetadata = nodeData.value;
            if (oldPartitionMetadata == null) {
                throw new StoreException("partition metadata value missing");
            }

            if (oldPartitionMetadata.generation <= generation) {
                PartitionMetadata newPartitionMetadata =
                    new PartitionMetadata(generation, oldPartitionMetadata.sessionId + 1, oldPartitionMetadata.replicaStates);

                try {
                    zkClient.setData(znode, newPartitionMetadata, serializer, nodeData.stat.getVersion());

                    return newPartitionMetadata;
                } catch (KeeperException.BadVersionException ex) {
                    // Ignore and retry
                    retryInterval = backoffTimer.backoff(retryInterval);
                }
            } else {
                throw new GenerationMismatchException();
            }
        }
    }

}
