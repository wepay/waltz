package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.exception.ReplicaReaderException;
import com.wepay.waltz.store.exception.SessionClosedException;
import com.wepay.waltz.store.internal.metadata.ReplicaId;
import com.wepay.waltz.test.util.IntegrationTestHelper;
import io.netty.handler.ssl.SslContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ReplicaSessionTest {

    @Test
    public void testInitialWrite() throws Exception {
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/storage/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "1");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);

        ReplicaId replicaId = new ReplicaId(0, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, 0));
        ReplicaConnectionFactoryImpl connectionFactory = null;

        try {
            helper.startZooKeeperServer();
            helper.startWaltzStorage(true);
            helper.setWaltzStorageAssignment(true);

            UUID key = helper.getClusterKey();
            SslContext sslContext = Utils.getSslContext(helper.getSslConfigPath(), WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
            ConnectionConfig config = TestUtils.makeConnectionConfig(1, key, sslContext);
            connectionFactory = new ReplicaConnectionFactoryImpl(helper.getStorageConnectString(), config);

            MockStoreSession mockStoreSession = new MockStoreSession(0, -1L, -1L);

            ReplicaSession replicaSession = new ReplicaSession(replicaId, 2, config, connectionFactory);
            RecoveryManager recoveryManager = TestUtils.mockRecoveryManager(1, -1L);
            replicaSession.open(recoveryManager, mockStoreSession);

            ReplicaReader reader = replicaSession.reader;

            recoveryManager.highWaterMark();

            ArrayList<StoreAppendRequest> requests = TestUtils.makeStoreAppendRequests(0, 10L);

            assertEquals(-1L, maxTransactionId(reader));

            Voting voting = new Voting(1, 1);
            replicaSession.append(0L, requests, voting);
            voting.await();

            assertEquals(9L, maxTransactionId(reader));

        } finally {
            helper.closeAll();
            if (connectionFactory != null) {
                connectionFactory.close();
            }
        }
    }

    @Test
    public void testCatchUp() throws Exception {
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/storage/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "1");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        IntegrationTestHelper helper = new IntegrationTestHelper(properties);

        ReplicaId replicaId = new ReplicaId(0, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, 0));
        ReplicaConnectionFactoryImpl connectionFactory = null;

        try {
            helper.startZooKeeperServer();
            helper.startWaltzStorage(true);
            helper.setWaltzStorageAssignment(true);

            UUID key = helper.getClusterKey();
            SslContext sslContext = Utils.getSslContext(helper.getSslConfigPath(), WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
            ConnectionConfig config = TestUtils.makeConnectionConfig(1, key, sslContext);
            connectionFactory = new ReplicaConnectionFactoryImpl(helper.getStorageConnectString(), config);

            MockStoreSession mockStoreSession = new MockStoreSession(0, -1L, -1L);

            ReplicaSession replicaSession = new ReplicaSession(replicaId, 2, config, connectionFactory);

            RecoveryManager recoveryManager = TestUtils.mockRecoveryManager(1, -1L);
            replicaSession.open(recoveryManager, mockStoreSession);

            recoveryManager.highWaterMark();

            ReplicaReader reader = replicaSession.reader;
            ReplicaWriter writer = replicaSession.writer;

            assertEquals(-1L, maxTransactionId(reader));

            ArrayList<StoreAppendRequest> requests;

            // Block the replica session task by synchronizing on the writer
            synchronized (writer) {
                // ==> 9
                mockStoreSession.insertTransactions(0L, 10L);
                mockStoreSession.setHighWaterMark(9L);

                // The following append request won't immediately take effect since the replica session task is blocked
                requests = TestUtils.makeStoreAppendRequests(0L, 10L);
                replicaSession.append(0L, requests, new Voting(1, 1));
                assertEquals(-1L, maxTransactionId(reader));

                // ==> 19
                mockStoreSession.insertTransactions(10L, 20L);
                mockStoreSession.setHighWaterMark(19L);

                // The following append request won't immediately take effect since reads are blocked by mockStoreSession
                requests = TestUtils.makeStoreAppendRequests(10L, 20L);
                replicaSession.append(10L, requests, new Voting(1, 1));
                assertEquals(-1L, maxTransactionId(reader));
            }
            // The replica session task is unblocked

            int attempts = 10;
            while (maxTransactionId(reader) < 19L && attempts > 0) {
                Thread.sleep(20);
                attempts--;
            }

            assertEquals(19L, maxTransactionId(reader));

            // Block the replica session task by synchronizing on the writer
            synchronized (writer) {
                // ==> 29
                mockStoreSession.insertTransactions(20L, 30L);
                mockStoreSession.setHighWaterMark(29L);

                // The following append request won't immediately take effect since reads are blocked by mockStoreSession
                requests = TestUtils.makeStoreAppendRequests(20L, 30L);
                replicaSession.append(20L, requests, new Voting(1, 1));
                assertEquals(19L, maxTransactionId(reader));
            }
            // The replica session task is unblocked

            // Start catching up
            // ==> 39
            mockStoreSession.insertTransactions(30L, 40L);
            mockStoreSession.setHighWaterMark(39L);

            // ==> 49
            mockStoreSession.insertTransactions(40L, 50L);
            mockStoreSession.setHighWaterMark(49L);

            requests = TestUtils.makeStoreAppendRequests(30L, 40L);
            replicaSession.append(30L, requests, new Voting(1, 1));

            requests = TestUtils.makeStoreAppendRequests(40L, 50L);
            replicaSession.append(40L, requests, new Voting(1, 1));

            attempts = 10;
            while (maxTransactionId(reader) < 49L && attempts > 0) {
                Thread.sleep(20);
                attempts--;
            }
            assertEquals(49L, maxTransactionId(reader));

            replicaSession.close();
            mockStoreSession.close();

        } finally {
            if (connectionFactory != null) {
                connectionFactory.close();
            }

            helper.closeAll();
        }
    }

    private long maxTransactionId(ReplicaReader reader) throws ReplicaReaderException {
        long maxTransactionId = -1L;
        while (reader.getRecord(maxTransactionId + 1) != null) {
            maxTransactionId++;
        }
        return maxTransactionId;
    }

    private static class MockStoreSession implements StoreSession {

        private final int generation;
        private final HashMap<Long, Record> transactions = new HashMap<>();

        private long highWaterMark;
        private long lowWaterMark;
        private volatile boolean running = true;

        MockStoreSession(int generation, long highWaterMark, long lowWaterMark) {
            this.generation = generation;
            this.highWaterMark = highWaterMark;
            this.lowWaterMark = lowWaterMark;
        }

        @Override
        public void close() {
            synchronized (this) {
                running = false;
                notifyAll();
            }
        }

        @Override
        public int generation() {
            return generation;
        }

        @Override
        public long highWaterMark() {
            return highWaterMark;
        }

        @Override
        public long lowWaterMark() {
            return lowWaterMark;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public void append(StoreAppendRequest request) throws SessionClosedException, GenerationMismatchException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numPendingAppends() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resolveAllAppendRequests(long highWaterMark) {

        }

        @Override
        public long flush() throws SessionClosedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record getRecord(long transactionId) throws SessionClosedException {
            if (transactionId <= highWaterMark) {
                return getRecordUnsafe(transactionId);
            } else {
                return null;
            }
        }

        @Override
        public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws SessionClosedException {
            ArrayList<Record> list = new ArrayList<>();
            if (transactionId <= highWaterMark) {
                for (int i = 0; i < maxNumRecords; i++) {
                    Record record = getRecordUnsafe(transactionId + i);
                    if (record != null) {
                        list.add(record);
                    } else {
                        break;
                    }
                }
                return list;
            } else {
                return list;
            }
        }

        @Override
        public RecordHeader getRecordHeader(long transactionId) throws SessionClosedException {
            Record record = getRecord(transactionId);

            if (record != null) {
                return new RecordHeader(transactionId, record.reqId, record.header);
            } else {
                return null;
            }
        }

        @Override
        public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws SessionClosedException {
            ArrayList<RecordHeader> list = new ArrayList<>();

            for (Record record : getRecordList(transactionId, maxNumRecords)) {
                list.add(new RecordHeader(transactionId, record.reqId, record.header));
            }

            return list;
        }

        private Record getRecordUnsafe(long transactionId) throws SessionClosedException {
            if (running) {
                return transactions.get(transactionId);
            }
            throw new SessionClosedException();
        }

        void setHighWaterMark(long highWaterMark) {
            this.highWaterMark = highWaterMark;
        }

        void insertTransactions(long startTransactionId, long endTransactionId) {
            // Insert transactions
            for (long transactionId = startTransactionId; transactionId < endTransactionId; transactionId++) {
                transactions.put(transactionId, TestUtils.record(transactionId));
            }
        }

    }
}
