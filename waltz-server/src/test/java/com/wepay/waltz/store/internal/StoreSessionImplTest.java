package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.internal.metadata.PartitionMetadata;
import com.wepay.waltz.store.internal.metadata.PartitionMetadataSerializer;
import com.wepay.waltz.store.internal.metadata.ReplicaId;
import com.wepay.waltz.store.internal.metadata.ReplicaState;
import com.wepay.waltz.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class StoreSessionImplTest {

    private static final int NUM_REPLICAS = 3;

    private final Random rand = new Random();

    @Test
    public void testAppend() throws Exception {
        int partitionId = 0;
        int generation = 30;
        long sessionId = 20;
        long firstTransactionId = rand.nextInt(100);
        int seqNum = rand.nextInt(1000);

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            ZNode root = zkClient.createPath(new ZNode("/test/store"));
            ZNode znode = new ZNode(root, Integer.toString(partitionId));

            TestReplicaSessionManager replicaSessionManager = new TestReplicaSessionManager(1, NUM_REPLICAS);
            replicaSessionManager.setLastSessionInfo(partitionId, sessionId - 1L, -1L);
            replicaSessionManager.setMaxTransactionId(partitionId, firstTransactionId - 1);

            Map<ReplicaId, ReplicaState> replicaStates = new HashMap<>();
            for (int i = 0; i < NUM_REPLICAS; i++) {
                ReplicaId replicaId = new ReplicaId(partitionId, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, i));
                replicaStates.put(replicaId, new ReplicaState(replicaId, sessionId - 1L, ReplicaState.UNRESOLVED));
            }

            zkClient.create(
                znode,
                new PartitionMetadata(generation - 1, sessionId, replicaStates),
                PartitionMetadataSerializer.INSTANCE,
                CreateMode.PERSISTENT
            );

            try {
                ArrayList<ReplicaSession> replicaSessions = replicaSessionManager.getReplicaSessions(partitionId, sessionId);
                StoreSessionImpl session =
                    new StoreSessionImpl(
                        partitionId,
                        generation,
                        sessionId,
                        WaltzServerConfig.DEFAULT_STORE_SESSION_BATCH_SIZE,
                        replicaSessions,
                        zkClient,
                        znode
                    );
                session.open();
                try {
                    // Test generation mismatch
                    try {
                        Record record = TestUtils.record(new ReqId(1, generation - 1, partitionId, seqNum++), 0);

                        TestUtils.syncAppend(session, record);
                        fail();

                    } catch (GenerationMismatchException ex) {
                        // Ignore
                    }

                    // Test appending transaction records
                    Record[] records = new Record[10];
                    for (int i = 0; i < 10; i++) {
                        records[i] = TestUtils.record(new ReqId(1, generation, partitionId, seqNum++), firstTransactionId + i);

                        assertEquals(firstTransactionId + i, TestUtils.syncAppend(session, records[i]));
                        assertEquals(firstTransactionId + i, session.highWaterMark());
                    }

                    for (int i = 0; i < 10; i++) {
                        long j = rand.nextInt(10);
                        Record record = records[(int) j];
                        RecordHeader ret = session.getRecordHeader(firstTransactionId + j);
                        if (ret == null) {
                            System.out.println("null");
                        }
                        assertEquals(new RecordHeader(firstTransactionId + j, record.reqId, record.header), ret);
                    }
                    for (int i = 0; i < 10; i++) {
                        long j = rand.nextInt(10);
                        assertEquals(records[(int) j], session.getRecord(firstTransactionId + j));
                    }

                    assertNull(session.getRecordHeader(firstTransactionId + 10));
                    assertNull(session.getRecord(firstTransactionId + 10));

                } finally {
                    session.close();
                }
            } finally {
                replicaSessionManager.close();
                zkClient.close();
            }
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

}
