package com.wepay.waltz.store.internal;

import com.wepay.waltz.store.TestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TestReplicaSessionManager extends ReplicaSessionManager {

    public static final String CONNECT_STRING_TEMPLATE = "test:%d";

    public final ArrayList<String> connectStrings;

    private Map<String, MockReplicaConnectionFactory> mockConnectionFactories;

    public TestReplicaSessionManager(int numPartitions, int numReplicas) throws Exception {
        this(numPartitions, makeConnectStrings(numReplicas));
    }

    private TestReplicaSessionManager(int numPartitions, ArrayList<String> connectStrings) throws Exception {
        super(
            TestUtils.makeReplicaAssignment(numPartitions, connectStrings),
            TestUtils.makeConnectionConfig(numPartitions, UUID.randomUUID())
        );
        this.connectStrings = connectStrings;
    }

    @Override
    protected ReplicaConnectionFactory createReplicaConnectionFactory(String connectString, ConnectionConfig config) {
        synchronized (this) {
            if (mockConnectionFactories == null) {
                mockConnectionFactories = new HashMap<>();
            }
            MockReplicaConnectionFactory factory = mockConnectionFactories.get(connectString);
            if (factory == null) {
                factory = new MockReplicaConnectionFactory(config.numPartitions);
                mockConnectionFactories.put(connectString, factory);
            }
            return factory;
        }
    }

    public MockReplicaConnectionFactory getReplicaConnectionFactory(String connectString) {
        synchronized (this) {
            MockReplicaConnectionFactory connectionFactory = mockConnectionFactories.get(connectString);
            if (connectionFactory == null) {
                throw new IllegalArgumentException("connection factory not found: " + connectString);
            }
            return connectionFactory;
        }
    }

    private static ArrayList<String> makeConnectStrings(int numReplicas) {
        ArrayList<String> connectStrings = new ArrayList<>();
        for (int i = 0; i < numReplicas; i++) {
            connectStrings.add(String.format(CONNECT_STRING_TEMPLATE, i));
        }
        return connectStrings;
    }

    void setLastSessionInfo(int partitionId, long lastSessionId, long lowWaterMark) {
        synchronized (this) {
            for (MockReplicaConnectionFactory factory : mockConnectionFactories.values()) {
                factory.setLastSessionInfo(partitionId, lastSessionId, lowWaterMark);
            }
        }
    }

    void setMaxTransactionId(int partitionId, long transactionId) {
        synchronized (this) {
            for (MockReplicaConnectionFactory factory : mockConnectionFactories.values()) {
                factory.setMaxTransactionId(partitionId, transactionId);
            }
        }
    }

}
