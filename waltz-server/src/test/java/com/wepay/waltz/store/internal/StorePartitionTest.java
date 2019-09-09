package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.BackoffTimer;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.test.util.ZooKeeperServerRunner;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class StorePartitionTest {

    private static final int NUM_REPLICAS = 5;

    private final Random rand = new Random();
    private final WaltzServerConfig config = new WaltzServerConfig(new Properties());
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
            StoreSessionManager storeSessionManager = new StoreSessionManager(partitionId, generation, replicaSessionManager, zkClient, znode);
            StorePartitionImpl partition = new StorePartitionImpl(storeSessionManager, config);

            try {
                long expectedHighWaterMark = -1L;

                assertEquals(expectedHighWaterMark, partition.highWaterMark());

                int numTransactions = 100;
                Record[] records = new Record[numTransactions];

                for (int i = 0; i < numTransactions; i++) {
                    // Randomly update generations
                    if (rand.nextInt(10) == 0) {
                        partition.generation(++generation);
                    }

                    ReqId reqId = new ReqId(1, generation, 1, seqNum++);
                    records[i] = TestUtils.record(reqId, i);

                    syncAppend(partition, records[i]);

                    assertEquals(++expectedHighWaterMark, partition.highWaterMark());
                }

                for (int i = 0; i < 50; i++) {
                    long transactionId = (long) rand.nextInt(numTransactions);
                    assertEquals(records[(int) transactionId], partition.getRecord(transactionId));
                }

            } finally {
                partition.close();

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
            StoreSessionManager storeSessionManager = new StoreSessionManager(partitionId, generation, replicaSessionManager, zkClient, znode);
            StorePartitionImpl partition = new StorePartitionImpl(storeSessionManager, config);

            try {
                long expectedHighWaterMark = -1L;

                assertEquals(expectedHighWaterMark, partition.highWaterMark());

                int numTransactions = 20;
                Record[] record = new Record[numTransactions];

                for (int i = 0; i < numTransactions; i++) {
                    if (i == 5) {
                        // Simulate shutting the first replica down

                        // Make sure all replicas are up to date first (otherwise deadlock is possible)
                        for (String connectString : replicaSessionManager.connectStrings) {
                            MockReplicaConnectionFactory connectionFactory  = replicaSessionManager.getReplicaConnectionFactory(connectString);
                            connectionFactory.await(0, expectedHighWaterMark);
                        }

                        MockReplicaConnectionFactory connectionFactory  = replicaSessionManager.getReplicaConnectionFactory("test:0");
                        connectionFactory.setReplicaDown();
                    }
                    if (i == 10) {
                        // Simulate starting the first replica up
                        MockReplicaConnectionFactory connectionFactory  = replicaSessionManager.getReplicaConnectionFactory("test:0");
                        connectionFactory.setReplicaUp();
                    }
                    if (i == 15) {
                        // Simulate restarting all replicas
                        for (String connectString : replicaSessionManager.connectStrings) {
                            MockReplicaConnectionFactory connectionFactory  = replicaSessionManager.getReplicaConnectionFactory(connectString);
                            connectionFactory.setReplicaDown();
                            connectionFactory.setReplicaUp();
                        }
                    }

                    ReqId reqId = new ReqId(1, generation, 0, seqNum++);
                    record[i] = TestUtils.record(reqId, i);

                    syncAppend(partition, record[i]);

                    assertEquals(++expectedHighWaterMark, partition.highWaterMark());
                }

                for (int i = 0; i < 20; i++) {
                    long transactionId = (long) rand.nextInt(numTransactions);
                    assertEquals(record[(int) transactionId], partition.getRecord(transactionId));
                }

            } finally {
                partition.close();
                replicaSessionManager.close();
                zkClient.close();
            }
        } finally {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private long syncAppend(StorePartition partition, Record record) throws Exception {
        long retryInterval = 10;
        int attempts = 0;

        while (true) {
            try {
                attempts++;
                State<Long> appendState = new State<>(null);
                StateChangeFuture<Long> f = appendState.watch();
                partition.append(record.reqId, record.header, record.data, record.checksum, appendState::set);
                while (!f.isDone()) {
                    partition.flush();
                }
                if (f.get() >= 0L) {
                    return f.get();
                }
            } catch (StoreException ex) {
                if (attempts < 5) {
                    retryInterval = backoffTimer.backoff(retryInterval);
                } else {
                    throw ex;
                }
            }
        }
    }

}
