package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.metadata.ReplicaAssignments;
import com.wepay.waltz.common.metadata.ReplicaId;
import com.wepay.zktools.zookeeper.NodeData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class handles the {@link ReplicaSession}s.
 */
public class ReplicaSessionManager {

    private final ConnectionConfig config;
    private Set<ReplicaId> replicaIds;
    private Map<String, ReplicaConnectionFactory> connectionFactories;

    /**
     * Class constructor.
     * @param assignments The {@link ReplicaAssignments}.
     * @param config The replica connection config.
     */
    public ReplicaSessionManager(ReplicaAssignments assignments, ConnectionConfig config) {
        this.config = config;
        this.replicaIds = createReplicaIds(assignments);
        this.connectionFactories = createConnectionFactories();
    }

    ArrayList<ReplicaSession> getReplicaSessions(int partitionId, long sessionId) {
        // Create new sessions
        synchronized (this) {
            ArrayList<ReplicaSession> replicaSessions = new ArrayList<>();
            for (ReplicaId replicaId: replicaIds) {
                String connectString = replicaId.storageNodeConnectString;
                if (replicaId.partitionId == partitionId) {
                    replicaSessions.add(new ReplicaSession(replicaId, sessionId, config, connectionFactories.get(connectString)));
                }
            }

            return replicaSessions;
        }
    }

    /**
     * Update ReplicaSessionManager based on ReplicaAssignments. First, it will close
     * connectionFactory, which will trigger recovery process. Then, it will update
     * replicaIds and connectionFactories, where ReplicaSession will be created from.
     * @param nodeData
     */
    public void updateReplicaSessionManager(NodeData<ReplicaAssignments> nodeData) {
        synchronized (this) {
            close();

            // update connectionFactories, where new ReplicaSession created from
            replicaIds = createReplicaIds(nodeData.value);
            connectionFactories = createConnectionFactories();
        }
    }

    /**
     * Closes the replica session manager.
     */
    public void close() {
        synchronized (this) {
            for (ReplicaConnectionFactory connectionFactory : connectionFactories.values()) {
                try {
                    connectionFactory.close();
                } catch (Throwable ex) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Returns an unmodifiable set of replica ids.
     * @return an unmodifiable set of replica ids
     */
    public Set<ReplicaId> getReplicaIds() {
        synchronized (this) {
            return Collections.unmodifiableSet(replicaIds);
        }
    }

    /**
     * Returns an unmodifiable map of connection factories keyed by connect strings.
     * @return an unmodifiable map of connection factories
     */
    public Map<String, ReplicaConnectionFactory> getConnectionFactories() {
        synchronized (this) {
            return Collections.unmodifiableMap(connectionFactories);
        }
    }

    protected ReplicaConnectionFactory createReplicaConnectionFactory(String connectString, ConnectionConfig config) {
        return new ReplicaConnectionFactoryImpl(connectString, config);
    }

    /**
     * Update replicaIds based on ReplicaAssignments.
     * @param assignments
     */
    private Set<ReplicaId> createReplicaIds(ReplicaAssignments assignments) {
        Set<ReplicaId> replicaIds = new HashSet<>();
        for (Map.Entry<String, int[]> entry : assignments.replicas.entrySet()) {
            String connectString = entry.getKey();
            for (int partitionId : entry.getValue()) {
                replicaIds.add(new ReplicaId(partitionId, connectString));
            }
        }
        return replicaIds;
    }

    /**
     * Update connectionFactories based on replicaIds.
     */
    private Map<String, ReplicaConnectionFactory> createConnectionFactories() {
        Map<String, ReplicaConnectionFactory> connectionFactories = new HashMap<>();
        for (ReplicaId replicaId : replicaIds) {
            String connectString = replicaId.storageNodeConnectString;
            connectionFactories.putIfAbsent(connectString, createReplicaConnectionFactory(connectString, config));
        }
        return connectionFactories;
    }

}
