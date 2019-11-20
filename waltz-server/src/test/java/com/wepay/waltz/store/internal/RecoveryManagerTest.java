package com.wepay.waltz.store.internal;

import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.common.metadata.store.internal.PartitionMetadata;
import com.wepay.waltz.common.metadata.store.internal.PartitionMetadataSerializer;
import com.wepay.waltz.common.metadata.store.internal.ReplicaId;
import com.wepay.waltz.common.metadata.store.internal.ReplicaState;
import com.wepay.waltz.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RecoveryManagerTest {

    private static final int NUM_PARTITIONS = 1;
    private static final int PARTITION_ID = 0;
    private static final int GENERATION = 10;
    private static final long SESSION_ID = 999;
    private static final long MIN_EXPECTED_HIGH_WATER_MARK = 120;
    private static final long MAX_EXPECTED_HIGH_WATER_MARK = 125;
    private static final long UNRESOLVED = ReplicaState.UNRESOLVED;

    private List<Boolean> allReplicasAvailable = Arrays.asList(true, true, true, true, true);

    @Test
    public void testBasic1() throws Exception {
        // Test recovery when all replicas are new.
        List<Long> metaLastSessionIds = Collections.emptyList();
        List<Long> metaHighWaterMarks = Collections.emptyList();
        List<Long> storageLastSessionIds = Arrays.asList(-1L, -1L, -1L, -1L, -1L);
        List<Long> storageLowWaterMarks = Arrays.asList(-1L, -1L, -1L, -1L, -1L);
        List<Long> storageMaxTransactionIds = Arrays.asList(-1L, -1L, -1L, -1L, -1L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable, -1L, -1L);
    }

    @Test
    public void testBasic2() throws Exception {
        // Test recovery when all replicas can vote.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 110L, 110L);
        List<Long> storageMaxTransactionIds = Arrays.asList(125L, 125L, 120L, 115L, 115L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testBasic3() throws Exception {
        // Test recovery when three replicas can vote.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 110L, 110L);
        List<Long> storageMaxTransactionIds = Arrays.asList(125L, 125L, 120L, 115L, 115L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testBasic4() throws Exception {
        // Test recovery when two replicas can vote.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, 110L, 110L, 110L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 105L, 105L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testBasic5() throws Exception {
        // Test recovery when one replica can vote.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, 110L, 110L, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 105L, 105L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testBasic6() throws Exception {
        // Test recovery when three replicas can vote. Dirty records should be truncated.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, 110L, 110L, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 130L, 105L, 105L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testRecoveryAfterRecoveryFailure1() throws Exception {
        // Test recovery when previous recovery failed.
        // The last three replicas are the resumed replicas.
        // For a successful recovery, majority of replicas should have the latest sessionId.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, 120L, 120L, 120L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(120L, 120L, 100L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 105L, 105L, 105L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testRecoveryAfterRecoveryFailure2() throws Exception {
        // Test recovery when previous recovery failed.
        // The last three replicas are the resumed replicas, but with different sessionId.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 997L, 996L, 996L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, 120L, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 996L, 996L);
        List<Long> storageLowWaterMarks = Arrays.asList(120L, 120L, 100L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 105L, 105L, 105L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testCatchUpReplicas() throws Exception {
        // Test recovery when the last two replicas are falling far behind.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> storageLowWaterMarks = Arrays.asList(120L, 120L, 120L, 120L, 120L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 110L, 100L);
        List<Boolean> replicaAvailability = Arrays.asList(false, false, true, true, true); // Disable the first two replicas

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testInconsistentStorage1() throws Exception {
        // Test recovery when third replica cannot vote due to inconsistent sessionId
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 100L, 100L, 100L);
        List<Long> maxTransactionIds = Arrays.asList(125L, 120L, 115L, 115L, 115L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, maxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testInconsistentStorage2() throws Exception {
        // Test recovery when third replica cannot vote due to inconsistent sessionId
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 100L, 100L, 100L);
        List<Long> maxTransactionIds = Arrays.asList(125L, 120L, 130L, 115L, 115L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, maxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testReplicaUnavailable1() throws Exception {
        // Test recovery when third replica is disabled.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(true, true, false, true, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaUnavailable2() throws Exception {
        // Test recovery when third and fourth replicas are disabled.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(true, true, false, false, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaUnavailable3() throws Exception {
        // Test recovery when first and fifth replicas are disabled.
        // All replicas can vote.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 110L, 110L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 115L, 115L, 110L);
        List<Boolean> replicaAvailability = Arrays.asList(false, true, true, true, false);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaUnavailable4() throws Exception {
        // Test recovery when fist and second replicas are disabled.
        // All replicas can vote.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 110L, 110L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(false, false, true, true, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaAdded1() throws Exception {
        // Test recovery when fourth replica is added.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(true, true, true, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaAdded2() throws Exception {
        // Test recovery when fourth and fifth replicas are added.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 100L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 115L, 115L, 115L);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, allReplicasAvailable);
    }

    @Test
    public void testReplicaAdded3() throws Exception {
        // Test recovery when first replica is disabled, while fourth replica is added.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(false, true, true, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaRemoved1() throws Exception {
        // Test recovery when first replica is removed.
        // When a replica is removed, set its availability to null.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(null, true, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaRemoved2() throws Exception {
        // Test recovery when first and second replicas are removed.
        // When a replica is removed, set its availability to null.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, 100L, 100L);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(null, null, true, true, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    @Test
    public void testReplicaReplaced() throws Exception {
        // Test recovery when first replica is removed, while fourth replica is added.
        // When a replica is removed, set its availability to null.
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 997L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 997L, 997L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 100L, 100L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(null, true, true, true);

        test0(metaLastSessionIds, metaHighWaterMarks, storageLastSessionIds, storageLowWaterMarks, storageMaxTransactionIds, replicaAvailability);
    }

    private void test0(
        List<Long> metaLastSessionIds,
        List<Long> metaHighWaterMarks,
        List<Long> storageLastSessionIds,
        List<Long> storageLowWaterMarks,
        List<Long> storageMaxTransactionIds,
        List<Boolean> replicaAvailability
    ) throws Exception {
        test0(
            metaLastSessionIds,
            metaHighWaterMarks,
            storageLastSessionIds,
            storageLowWaterMarks,
            storageMaxTransactionIds,
            replicaAvailability,
            MIN_EXPECTED_HIGH_WATER_MARK,
            MAX_EXPECTED_HIGH_WATER_MARK);
    }

    private void test0(
        List<Long> metaLastSessionIds,
        List<Long> metaHighWaterMarks,
        List<Long> storageLastSessionIds,
        List<Long> storageLowWaterMarks,
        List<Long> storageMaxTransactionIds,
        List<Boolean> replicaAvailability,
        long minExpectedHighWaterMark,
        long maxExpectedHighWaterMark
    ) throws Exception {
        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);
            ZNode znode = new ZNode("/test/partitions/0");

            // Number of replicas; not include the ones get removed.
            int numReplicas = (int) replicaAvailability.stream().filter(bool -> bool != null).count();
            int quorum = numReplicas / 2 + 1;
            RecoveryManager recoveryManager = new RecoveryManagerImpl(GENERATION, SESSION_ID, quorum, zkClient, znode);

            // Availabilities for replicas; not include the ones get removed.
            List<Boolean> availabilities = new ArrayList<>();
            ArrayList<MockReplicaConnectionFactory> factories = new ArrayList<>();
            ArrayList<ReplicaSession> replicaSessions = new ArrayList<>();

            try {
                TestUtils.setUpMetadata(GENERATION, SESSION_ID, metaLastSessionIds, metaHighWaterMarks, zkClient, znode);

                ConnectionConfig config = TestUtils.makeConnectionConfig(NUM_PARTITIONS, UUID.randomUUID());

                for (int i = 0; i < replicaAvailability.size(); i++) {
                    // When replica is removed, neither connection factory nor replica session will be created.
                    if (replicaAvailability.get(i) == null) {
                        continue;
                    }

                    MockReplicaConnectionFactory factory = new MockReplicaConnectionFactory(NUM_PARTITIONS);

                    long storageSessionId = storageLastSessionIds.get(i);
                    factory.setCurrentSession(PARTITION_ID, storageSessionId);
                    factory.appendRecords(PARTITION_ID, storageSessionId, TestUtils.records(0, storageMaxTransactionIds.get(i) + 1));
                    factory.setLastSessionInfo(PARTITION_ID, storageSessionId, storageLowWaterMarks.get(i));
                    factory.setMaxTransactionId(PARTITION_ID, storageMaxTransactionIds.get(i));
                    factory.setReplicaDown();

                    ReplicaId replicaId = new ReplicaId(PARTITION_ID, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, i));

                    availabilities.add(replicaAvailability.get(i));
                    factories.add(factory);
                    replicaSessions.add(new ReplicaSession(replicaId, SESSION_ID, config, factory));
                }

                for (ReplicaSession replicaSession : replicaSessions) {
                    replicaSession.open(recoveryManager, null);
                }

                for (int i = 0; i < numReplicas; i++) {
                    if (availabilities.get(i)) {
                        // make the replica available
                        factories.get(i).setReplicaUp();
                    }
                }

                recoveryManager.start(replicaSessions);

                // Check the replicas
                int numAvailable = 0;
                for (int i = 0; i < numReplicas; i++) {
                   if (availabilities.get(i)) {
                        numAvailable++;

                        replicaSessions.get(i).awaitRecovery();

                        MockReplicaConnectionFactory factory = factories.get(i);
                        assertEquals(
                            factory.getLowWaterMark(PARTITION_ID, SESSION_ID), factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                        );
                        assertTrue(
                            "compare low-water mark",
                            minExpectedHighWaterMark <= factory.getLowWaterMark(PARTITION_ID, SESSION_ID)
                                && maxExpectedHighWaterMark >= factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                        );
                        assertTrue(
                            "compare max transaction id mark",
                            minExpectedHighWaterMark <= factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                                && maxExpectedHighWaterMark >= factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                        );
                    }
                }

                // Check the metadata of available replicas
                assertTrue("timeout while wait for metadata", TestUtils.awaitMetadata(numAvailable, SESSION_ID, zkClient, znode));
                PartitionMetadata metadata = TestUtils.getMetadata(zkClient, znode);
                assertEquals(GENERATION, metadata.generation);
                assertEquals(SESSION_ID, metadata.sessionId);
                int count = 0;
                for (ReplicaState replicaState : metadata.replicaStates.values()) {
                    if (SESSION_ID == replicaState.sessionId) {
                        count++;
                        assertEquals(UNRESOLVED, replicaState.closingHighWaterMark);
                    }
                }
                assertEquals(numAvailable, count);

                // Make all replicas available
                for (int i = 0; i < numReplicas; i++) {
                    if (!availabilities.get(i)) {
                        factories.get(i).setReplicaUp();
                    }
                }

                // Check the replicas
                for (int i = 0; i < numReplicas; i++) {
                    replicaSessions.get(i).awaitRecovery();

                    MockReplicaConnectionFactory factory = factories.get(i);
                    assertEquals(
                        factory.getLowWaterMark(PARTITION_ID, SESSION_ID), factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                    );
                    assertTrue(
                        "compare low-water mark",
                        minExpectedHighWaterMark <= factory.getLowWaterMark(PARTITION_ID, SESSION_ID)
                            && maxExpectedHighWaterMark >= factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                    );
                    assertTrue(
                        "compare max transaction id mark",
                        minExpectedHighWaterMark <= factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                            && maxExpectedHighWaterMark >= factory.getMaxTransactionId(PARTITION_ID, SESSION_ID)
                    );
                }

                // Check the metadata
                assertTrue("timeout while wait for metadata", TestUtils.awaitMetadata(numReplicas, SESSION_ID, zkClient, znode));
                metadata = TestUtils.getMetadata(zkClient, znode);
                assertEquals(GENERATION, metadata.generation);
                assertEquals(SESSION_ID, metadata.sessionId);
                for (ReplicaState replicaState : metadata.replicaStates.values()) {
                    assertEquals(SESSION_ID, replicaState.sessionId);
                    assertEquals(UNRESOLVED, replicaState.closingHighWaterMark);
                }

            } finally {
                for (ReplicaSession replicaSession : replicaSessions) {
                    replicaSession.close();
                }

                zkClient.close();

                for (ReplicaConnectionFactory factory : factories) {
                    factory.close();
                }
            }
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testIdempotency() throws Exception {
        List<Long> metaLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> metaHighWaterMarks = Arrays.asList(UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED, UNRESOLVED);
        List<Long> storageLastSessionIds = Arrays.asList(998L, 998L, 998L, 998L, 998L);
        List<Long> storageLowWaterMarks = Arrays.asList(110L, 110L, 110L, 110L, 110L);
        List<Long> storageMaxTransactionIds = Arrays.asList(120L, 120L, 120L, 115L, 115L);
        List<Boolean> replicaAvailability = Arrays.asList(true, true, true, true, true);

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);
            ZNode znode = new ZNode("/test/partitions/0");

            int numReplicas = (int) replicaAvailability.stream().filter(bool -> bool != null).count();
            int quorum = numReplicas / 2 + 1;
            RecoveryManager recoveryManager = new RecoveryManagerImpl(GENERATION, SESSION_ID, quorum, zkClient, znode) {
                @Override
                protected void updateReplicaStates() throws RecoveryFailedException {
                    Uninterruptibly.sleep(100);
                    throw new RecoveryFailedException("intentionally failed to update replica states");
                }
            };

            ArrayList<MockReplicaConnectionFactory> factories = new ArrayList<>();
            ArrayList<ReplicaSession> replicaSessions = new ArrayList<>();

            try {
                TestUtils.setUpMetadata(GENERATION, SESSION_ID, metaLastSessionIds, metaHighWaterMarks, zkClient, znode);

                ConnectionConfig config = TestUtils.makeConnectionConfig(NUM_PARTITIONS, UUID.randomUUID());

                for (int i = 0; i < numReplicas; i++) {
                    MockReplicaConnectionFactory factory = new MockReplicaConnectionFactory(NUM_PARTITIONS);

                    long storageSessionId = storageLastSessionIds.get(i);
                    factory.setCurrentSession(PARTITION_ID, storageSessionId);
                    factory.appendRecords(PARTITION_ID, storageSessionId, TestUtils.records(0, storageMaxTransactionIds.get(i) + 1));
                    factory.setLastSessionInfo(PARTITION_ID, storageSessionId, storageLowWaterMarks.get(i));
                    factory.setMaxTransactionId(PARTITION_ID, storageMaxTransactionIds.get(i));
                    factory.setReplicaUp();

                    ReplicaId replicaId = new ReplicaId(PARTITION_ID, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, i));

                    factories.add(factory);
                    replicaSessions.add(new ReplicaSession(replicaId, SESSION_ID, config, factory));
                }

                for (ReplicaSession replicaSession : replicaSessions) {
                    replicaSession.open(recoveryManager, null);
                }
                recoveryManager.start(replicaSessions);

                // Check the replicas
                for (int i = 0; i < numReplicas; i++) {
                    replicaSessions.get(i).awaitRecovery();
                }

                for (ReplicaSession replicaSession : replicaSessions) {
                    replicaSession.close();
                }
                replicaSessions.clear();

                // Create a new session
                PartitionMetadata newMetadata = updatePartitionMetadata(GENERATION, zkClient, znode);

                long newSessionId = newMetadata.sessionId;

                for (int i = 0; i < numReplicas; i++) {
                    MockReplicaConnectionFactory factory = factories.get(i);

                    ReplicaId replicaId = new ReplicaId(PARTITION_ID, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, i));
                    replicaSessions.add(new ReplicaSession(replicaId, newSessionId, config, factory));
                }

                recoveryManager = new RecoveryManagerImpl(GENERATION, newSessionId, quorum, zkClient, znode);

                for (ReplicaSession replicaSession : replicaSessions) {
                    replicaSession.open(recoveryManager, null);
                }
                recoveryManager.start(replicaSessions);

                // Check the replicas
                for (int i = 0; i < numReplicas; i++) {
                    replicaSessions.get(i).awaitRecovery();

                    MockReplicaConnectionFactory factory = factories.get(i);
                    assertEquals(
                        factory.getLowWaterMark(PARTITION_ID, newSessionId), factory.getMaxTransactionId(PARTITION_ID, newSessionId)
                    );
                    assertTrue(
                        "compare low-water mark",
                        MIN_EXPECTED_HIGH_WATER_MARK <= factory.getLowWaterMark(PARTITION_ID, newSessionId)
                            && MAX_EXPECTED_HIGH_WATER_MARK >= factory.getMaxTransactionId(PARTITION_ID, newSessionId)
                    );
                    assertTrue(
                        "compare max transaction id mark",
                        MIN_EXPECTED_HIGH_WATER_MARK <= factory.getMaxTransactionId(PARTITION_ID, newSessionId)
                            && MAX_EXPECTED_HIGH_WATER_MARK >= factory.getMaxTransactionId(PARTITION_ID, newSessionId)
                    );
                }

                // Check the metadata
                assertTrue("timeout while wait for metadata", TestUtils.awaitMetadata(numReplicas, newSessionId, zkClient, znode));
                PartitionMetadata metadata = TestUtils.getMetadata(zkClient, znode);
                assertEquals(GENERATION, metadata.generation);
                assertEquals(newSessionId, metadata.sessionId);
                for (ReplicaState replicaState : metadata.replicaStates.values()) {
                    assertEquals(newSessionId, replicaState.sessionId);
                    assertEquals(UNRESOLVED, replicaState.closingHighWaterMark);
                }

            } finally {
                for (ReplicaSession replicaSession : replicaSessions) {
                    replicaSession.close();
                }

                zkClient.close();

                for (ReplicaConnectionFactory factory : factories) {
                    factory.close();
                }
            }
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private PartitionMetadata updatePartitionMetadata(int generation, ZooKeeperClient zkClient, ZNode znode) throws GenerationMismatchException, ZooKeeperClientException, KeeperException {
        NodeData<PartitionMetadata> nodeData = zkClient.getData(znode, PartitionMetadataSerializer.INSTANCE);

        PartitionMetadata oldPartitionMetadata = nodeData.value;
        if (oldPartitionMetadata == null) {
            throw new RuntimeException("partition metadata value missing");
        }

        if (oldPartitionMetadata.generation <= generation) {
            PartitionMetadata newPartitionMetadata =
                new PartitionMetadata(generation, oldPartitionMetadata.sessionId + 1, oldPartitionMetadata.replicaStates);

            zkClient.setData(znode, newPartitionMetadata, PartitionMetadataSerializer.INSTANCE, nodeData.stat.getVersion());
            return newPartitionMetadata;

        } else {
            throw new GenerationMismatchException();
        }
    }

}
