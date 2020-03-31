package com.wepay.waltz.store.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.util.DaemonThreadFactory;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.Store;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.common.metadata.ReplicaAssignments;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.metadata.StoreParams;
import com.wepay.waltz.store.exception.StoreSessionManagerException;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.WatcherHandle;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements {@link Store}.
 */
public class StoreImpl implements Store {

    private static final Logger logger = Logging.getLogger(StoreImpl.class);

    private final ZooKeeperClient zkClient;
    private final ZNode storeRoot;
    private final ReplicaSessionManager replicaSessionManager;
    private final WaltzServerConfig config;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final WatcherHandle assignmentWatcherHandle;

    private final Map<Integer, StoreSessionManager> storeSessionManagers = new ConcurrentHashMap<>();
    private final ExecutorService asyncTaskExecutor = Executors.newSingleThreadExecutor(DaemonThreadFactory.INSTANCE);

    /**
     * Class constructor.
     * @param zkClient The ZooKeeperClient used for Waltz Cluster.
     * @param storeRoot Path to the store ZNode.
     * @param config Waltz server config.
     * @throws StoreException thrown if it fails to get the store metadata.
     */
    public StoreImpl(ZooKeeperClient zkClient, ZNode storeRoot, WaltzServerConfig config) throws StoreException {
        try {
            this.zkClient = zkClient;
            this.storeRoot = storeRoot;
            this.config = config;

            StoreMetadata storeMetadata = new StoreMetadata(zkClient, storeRoot);

            StoreParams storeParams = storeMetadata.getStoreParams();
            UUID key = storeParams.key;
            int numPartitions = storeParams.numPartitions;

            ReplicaAssignments replicaAssignments = storeMetadata.getReplicaAssignments();
            this.replicaSessionManager = new ReplicaSessionManager(replicaAssignments, new ConnectionConfig(key, numPartitions, config));
            this.assignmentWatcherHandle = storeMetadata.watchReplicaAssignments(this::onReplicaAssignmentsUpdate);
        } catch (Exception ex) {
            logger.error("failed to create store instance", ex);
            throw new StoreException("failed to create store instance", ex);
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            assignmentWatcherHandle.close();
            replicaSessionManager.close();
            storeSessionManagers.clear();
            asyncTaskExecutor.shutdownNow();
        }
    }

    @Override
    public StorePartition getPartition(int partitionId, int generation) {
        try {
            ZNode partitionRoot = new ZNode(storeRoot, StoreMetadata.PARTITION_ZNODE_NAME);
            ZNode znode = new ZNode(partitionRoot, Integer.toString(partitionId));

            StoreSessionManager storeSessionManager =
                new StoreSessionManager(
                    partitionId,
                    generation,
                    (int) config.get(WaltzServerConfig.MAX_BATCH_SIZE),
                    replicaSessionManager,
                    zkClient,
                    znode
                );

            storeSessionManagers.put(partitionId, storeSessionManager);
            return new StorePartitionImpl(storeSessionManager, config);
        } catch (IllegalArgumentException ex) {
            logger.error("failed to get a partition", ex);
            throw ex;
        }
    }

    /**
     * Returns an unmodifiable set of replica ids.
     * @return an unmodifiable set of replica ids
     */
    public Set<ReplicaId> getReplicaIds() {
        // replicaSessionManager.getReplicaIds() returns an unmodifiable set
        return replicaSessionManager.getReplicaIds();
    }

    /**
     * Returns {@link ConnectionConfig}
     * @return {@code ConnectionConfig}
     */
    public ConnectionConfig getConnectionConfig() {
        return replicaSessionManager.getConnectionConfig();
    }

    public void onPartitionRemoved(int partitionId) {
        storeSessionManagers.computeIfPresent(partitionId, (key, storeSessionManager) ->
            storeSessionManager.isClosed() ? null : storeSessionManager
        );
    }

    private void onReplicaAssignmentsUpdate(NodeData<ReplicaAssignments> nodeData) {
        synchronized (this) {
            Set<ReplicaId> replicaIds = getReplicaIds();
            Set<ReplicaId> newReplicaIds = ReplicaSessionManager.createReplicaIds(nodeData.value);

            if (newReplicaIds.equals(replicaIds)) {
                logger.debug("ReplicaId sets are the same, current:{}, new:{}", replicaIds, newReplicaIds);
                return;
            }

            replicaSessionManager.updateReplicaSessionManager(nodeData);

            asyncTaskExecutor.execute(() -> {
                for (StoreSessionManager sessionManager : storeSessionManagers.values()) {
                    if (!sessionManager.isClosed()) {
                        StoreSession currentSession = getStoreSession(sessionManager);

                        if (currentSession != null) {
                            currentSession.close();
                            getStoreSession(sessionManager);
                        }
                    }
                }
            });
        }
    }

    private StoreSession getStoreSession(StoreSessionManager storeSessionManager) {
        StoreSession storeSession = null;

        try {
            storeSession = storeSessionManager.getStoreSession();
        } catch (GenerationMismatchException | StoreSessionManagerException | RecoveryFailedException ex) {
            logger.warn("Failed to get StoreSession", ex);
        }
        return storeSession;
    }
}
