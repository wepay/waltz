package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.BackoffTimer;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.exception.SessionClosedException;
import com.wepay.waltz.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StoreSessionManagerTest {

    private static final int NUM_REPLICAS = 5;

    private final Random rand = new Random();
    private final BackoffTimer backoffTimer = new BackoffTimer(1000);

    @Test
    public void testAppend() throws Exception {
        int partitionId = 0;
        int generation = 0;
        int seqNum = rand.nextInt(1000);

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            ZNode root = zkClient.createPath(new ZNode("/test/store"));
            ZNode znode = new ZNode(root, Integer.toString(partitionId));

            ReplicaSessionManager replicaSessionManager = new TestReplicaSessionManager(1, NUM_REPLICAS);

            StoreSessionManager storeSessionManager =
                new StoreSessionManager(
                    partitionId,
                    generation,
                    WaltzServerConfig.DEFAULT_STORE_SESSION_BATCH_SIZE,
                    replicaSessionManager,
                    zkClient,
                    znode
                );
            StoreSession session;
            try {
                long expectedHighWaterMark = -1L;

                session = storeSessionManager.getStoreSession();
                assertEquals(expectedHighWaterMark, session.highWaterMark());

                final int numTransactions = 100;

                session = storeSessionManager.getStoreSession(generation);

                assertTrue(session.isWritable());

                for (int i = 0; i < numTransactions; i++) {
                    StoreSession lastSession = null;

                    // Randomly update generations
                    if (rand.nextInt(10) == 0) {
                        storeSessionManager.generation(++generation);

                        lastSession = session;
                    }

                    ReqId reqId = new ReqId(1, generation, 1, seqNum++);
                    Record record = TestUtils.record(reqId, i);

                    session = storeSessionManager.getStoreSession(generation);

                    if (lastSession != null) {
                        try {
                            assertEquals(-1L, TestUtils.syncAppend(lastSession, record));

                        } catch (SessionClosedException ex) {
                            // Ignore
                        }
                        assertFalse(lastSession.isWritable());
                    }

                    TestUtils.syncAppend(session, record);

                    session = storeSessionManager.getStoreSession();
                    assertEquals(++expectedHighWaterMark, session.highWaterMark());
                }

                // Verify transaction bodies in the store
                session = storeSessionManager.getStoreSession();
                for (int i = 0; i < numTransactions; i++) {
                    Record record = session.getRecord(i);
                    byte[] expected = TestUtils.data(i);
                    byte[] data =  record.data;

                    assertTrue(Arrays.equals(expected, data));
                }

            } finally {
                storeSessionManager.close();
                replicaSessionManager.close();
                zkClient.close();
            }

        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testStorageRestart() throws Exception {
        int partitionId = 0;
        int generation = 0;
        int seqNum = rand.nextInt(1000);

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String zkConnectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(zkConnectString, 30000);

            ZNode root = zkClient.createPath(new ZNode("/test/store"));
            ZNode znode = new ZNode(root, Integer.toString(partitionId));

            TestReplicaSessionManager replicaSessionManager = new TestReplicaSessionManager(1, NUM_REPLICAS);

            StoreSessionManager storeSessionManager =
                new StoreSessionManager(
                    partitionId,
                    generation,
                    WaltzServerConfig.DEFAULT_STORE_SESSION_BATCH_SIZE,
                    replicaSessionManager,
                    zkClient,
                    znode
                );
            StoreSession session = null;
            try {
                long expectedHighWaterMark = -1L;

                session = storeSessionManager.getStoreSession();
                assertEquals(expectedHighWaterMark, session.highWaterMark());

                final int numTransactions = 20;
                final Record[] records = new Record[numTransactions];
                int failureCount = 0;

                for (int i = 0; i < numTransactions; i++) {
                    if (i == 5) {
                        // Simulate shutting the first replica down

                        // Make sure all replicas are up to date first (otherwise deadlock is possible)
                        for (String connectString : replicaSessionManager.connectStrings) {
                            MockReplicaConnectionFactory connectionFactory  = replicaSessionManager.getReplicaConnectionFactory(connectString);
                            connectionFactory.await(0, expectedHighWaterMark);
                        }

                        MockReplicaConnectionFactory connectionFactory = replicaSessionManager.getReplicaConnectionFactory("test:0");
                        connectionFactory.setReplicaDown();
                    }
                    if (i == 10) {
                        // Simulate starting the first replica up
                        MockReplicaConnectionFactory connectionFactory = replicaSessionManager.getReplicaConnectionFactory("test:0");
                        connectionFactory.setReplicaUp();
                    }
                    if (i == 15) {
                        // Simulate restarting all replicas
                        for (String connectString : replicaSessionManager.connectStrings) {
                            MockReplicaConnectionFactory connectionFactory = replicaSessionManager.getReplicaConnectionFactory(connectString);
                            connectionFactory.setReplicaDown();
                            connectionFactory.setReplicaUp();
                        }
                    }

                    ReqId reqId = new ReqId(1, generation, 1, seqNum++);
                    records[i] = TestUtils.record(reqId, i);

                    long retryInterval = 10;
                    int attempts = 0;
                    while (true) {
                        try {
                            attempts++;

                            session = storeSessionManager.getStoreSession(generation);

                            if (attempts > 0 && expectedHighWaterMark + 1 == session.highWaterMark()) {
                                break;
                            }

                            if (TestUtils.syncAppend(session, records[i]) >= 0) {
                                break;
                            }

                        } catch (Exception ex) {
                            if (attempts < 10) {
                                retryInterval = backoffTimer.backoff(retryInterval);
                            } else {
                                throw ex;
                            }
                        }

                        failureCount++;
                    }

                    session = storeSessionManager.getStoreSession();
                    assertEquals(++expectedHighWaterMark, session.highWaterMark());
                }

                for (int i = 0; i < 20; i++) {
                    while (true) {
                        try {
                            session = storeSessionManager.getStoreSession();
                            long transactionId = (long) rand.nextInt(numTransactions);
                            assertEquals(records[(int) transactionId], session.getRecord(transactionId));
                            break;
                        } catch (SessionClosedException ex) {
                            // retry...
                            Uninterruptibly.sleep(10);
                        }
                    }
                }

                // Make sure we actually tested write failures
                assertTrue(failureCount > 0);

            } finally {
                storeSessionManager.close();
                replicaSessionManager.close();
                zkClient.close();
            }
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    @Test
    public void testSingleWriterGuarantee() throws Exception {
        int partitionId = 0;
        int generation = 0;
        int seqNum = rand.nextInt(1000);

        ZooKeeperServerRunner zooKeeperServerRunner = new ZooKeeperServerRunner(0);
        try {
            String connectString = zooKeeperServerRunner.start();
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(connectString, 30000);

            ZNode root = zkClient.createPath(new ZNode("/test/store"));
            ZNode znode = new ZNode(root, Integer.toString(partitionId));

            TestReplicaSessionManager replicaSessionManager = new TestReplicaSessionManager(1, NUM_REPLICAS);
            StoreSessionManager storeSessionManager1 =
                new StoreSessionManager(
                    partitionId,
                    generation,
                    WaltzServerConfig.DEFAULT_STORE_SESSION_BATCH_SIZE,
                    replicaSessionManager,
                    zkClient,
                    znode
                );
            StoreSessionManager storeSessionManager2 = null;
            StoreSession session1;
            StoreSession session2;
            try {
                long expectedHighWaterMark = -1L;

                session1 = storeSessionManager1.getStoreSession();
                assertEquals(expectedHighWaterMark, session1.highWaterMark());

                int numTransactions = 10;

                session1 = storeSessionManager1.getStoreSession(generation);

                assertTrue(session1.isWritable());

                for (int i = 0; i < numTransactions; i++) {
                    ReqId reqId = new ReqId(1, generation, 1, seqNum++);
                    Record record = TestUtils.record(reqId, i);
                    session1 = storeSessionManager1.getStoreSession(generation);
                    TestUtils.syncAppend(session1, record);

                    assertEquals(++expectedHighWaterMark, session1.highWaterMark());
                }

                // Open another session manager with new generation
                storeSessionManager2 =
                    new StoreSessionManager(
                        partitionId,
                        generation + 1,
                        WaltzServerConfig.DEFAULT_STORE_SESSION_BATCH_SIZE,
                        replicaSessionManager,
                        zkClient,
                        znode
                    );

                session2 = storeSessionManager2.getStoreSession();
                assertEquals(expectedHighWaterMark, session2.highWaterMark());

                try {
                    ReqId reqId = new ReqId(1, generation, 1, seqNum++);
                    Record record = TestUtils.record(reqId, 1000);

                    session1 = storeSessionManager1.getStoreSession(generation);
                    assertEquals(-1L, TestUtils.syncAppend(session1, record));

                }  catch (GenerationMismatchException | SessionClosedException ex) {
                    // Ignore
                }

                generation++;
                for (int i = 0; i < numTransactions; i++) {
                    ReqId reqId = new ReqId(1, generation, 1, seqNum++);
                    Record record = TestUtils.record(reqId, i);
                    session2 = storeSessionManager2.getStoreSession(generation);
                    TestUtils.syncAppend(session2, record);

                    assertEquals(++expectedHighWaterMark, session2.highWaterMark());
                }

                for (int i = 0; i < numTransactions; i++) {
                    Record record = session2.getRecord(i);
                    byte[] expected = TestUtils.data(i);
                    byte[] data =  record.data;

                    assertTrue(Arrays.equals(expected, data));
                }

            } finally {
                storeSessionManager1.close();
                if (storeSessionManager2 != null) {
                    storeSessionManager2.close();
                }

                replicaSessionManager.close();

                zkClient.close();
            }
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

}
