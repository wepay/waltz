package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.storage.WaltzStorageConfig;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.test.util.IntegrationTestHelper;
import io.netty.handler.ssl.SslContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReplicaConnectionImplTest {

    private IntegrationTestHelper helper;
    private ReplicaConnectionFactoryImpl connectionFactory;

    @Before
    public void setup() throws Exception {
        final long segmentSizeThreshold = 400L;

        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/storage/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "2");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");
        properties.setProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD, String.valueOf(segmentSizeThreshold));

        helper = new IntegrationTestHelper(properties);
        helper.startZooKeeperServer();
        helper.startWaltzStorage(true);
        helper.setWaltzStorageAssignment(true);

        UUID key = helper.getClusterKey();
        SslContext sslContext = Utils.getSslContext(helper.getSslConfigPath(), WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
        ConnectionConfig config = TestUtils.makeConnectionConfig(2, key, sslContext);

        String connectString = helper.getStorageConnectString();
        connectionFactory = new ReplicaConnectionFactoryImpl(connectString, config);
    }

    @After
    public void teardown() throws Exception {
        if (connectionFactory != null) {
            connectionFactory.close();
            connectionFactory = null;
        }

        helper.closeAll();
        helper = null;
    }

    @Test
    public void testBasicReadWrite() throws Exception {
        try (ReplicaConnection replicaConnection0 = connectionFactory.get(0, 0);
             ReplicaConnection replicaConnection1 = connectionFactory.get(1, 0)) {
            SessionInfo sessionInfo = replicaConnection0.lastSessionInfo();
            assertEquals(-1L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            sessionInfo = replicaConnection1.lastSessionInfo();
            assertEquals(-1L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            replicaConnection0.setLowWaterMark(-1L);
            replicaConnection1.setLowWaterMark(-1L);

            ArrayList<Record> allRecords0 = new ArrayList<>();
            ArrayList<Record> allRecords1 = new ArrayList<>();

            Random rand = new Random();
            int transactionId;

            // Partition 0

            transactionId = 0;
            while (transactionId < 100) {
                int n = rand.nextInt(50);
                ArrayList<Record> records = makeRecords(transactionId, transactionId + n);
                transactionId += records.size();
                replicaConnection0.appendRecords(records);
                allRecords0.addAll(records);
            }

            assertEquals(allRecords0.size() - 1, replicaConnection0.getMaxTransactionId());
            assertEquals(allRecords1.size() - 1, replicaConnection1.getMaxTransactionId());

            for (Record record : allRecords0) {
                Record returnedRecord = replicaConnection0.getRecord(record.transactionId);

                assertEquals(record.transactionId, returnedRecord.transactionId);
                assertEquals(record.reqId, returnedRecord.reqId);
                assertEquals(record.header, returnedRecord.header);
                assertTrue(Arrays.equals(record.data, returnedRecord.data));
                assertEquals(record.checksum, returnedRecord.checksum);
            }

            for (Record record : allRecords0) {
                RecordHeader returnedRecordHeader = replicaConnection0.getRecordHeader(record.transactionId);

                assertEquals(record.transactionId, returnedRecordHeader.transactionId);
                assertEquals(record.reqId, returnedRecordHeader.reqId);
                assertEquals(record.header, returnedRecordHeader.header);
            }

            // Partition 1

            transactionId = 0;
            while (transactionId < 100) {
                int n = rand.nextInt(50);
                ArrayList<Record> records = makeRecords(transactionId, transactionId + n);
                transactionId += records.size();
                replicaConnection1.appendRecords(records);
                allRecords1.addAll(records);
            }

            assertEquals(allRecords0.size() - 1, replicaConnection0.getMaxTransactionId());
            assertEquals(allRecords1.size() - 1, replicaConnection1.getMaxTransactionId());

            for (Record record : allRecords1) {
                Record returnedRecord = replicaConnection1.getRecord(record.transactionId);

                assertEquals(record.transactionId, returnedRecord.transactionId);
                assertEquals(record.reqId, returnedRecord.reqId);
                assertEquals(record.header, returnedRecord.header);
                assertTrue(Arrays.equals(record.data, returnedRecord.data));
                assertEquals(record.checksum, returnedRecord.checksum);
            }

            for (Record record : allRecords1) {
                RecordHeader returnedRecordHeader = replicaConnection1.getRecordHeader(record.transactionId);

                assertEquals(record.transactionId, returnedRecordHeader.transactionId);
                assertEquals(record.reqId, returnedRecordHeader.reqId);
                assertEquals(record.header, returnedRecordHeader.header);
            }
        }

        try (ReplicaConnection replicaConnection2 = connectionFactory.get(0, 0);
             ReplicaConnection replicaConnection3 = connectionFactory.get(1, 0)) {
            SessionInfo sessionInfo = replicaConnection2.lastSessionInfo();
            assertEquals(0L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            sessionInfo = replicaConnection3.lastSessionInfo();
            assertEquals(0L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);
        }
    }

    @Test
    public void testTransactionOutOfOrder() throws Exception {
        try (ReplicaConnection replicaConnection = connectionFactory.get(0, 0)) {
            replicaConnection.setLowWaterMark(-1L);

            int transactionId = 0;
            ArrayList<Record> records = makeRecords(transactionId, transactionId + 20);
            replicaConnection.appendRecords(records);

            // one overlapping transaction id
            transactionId--;
            records = makeRecords(transactionId, transactionId + 20);

            try {
                replicaConnection.appendRecords(records);
                fail();

            } catch (StorageRpcException ex) {
                // OK
            }
        }
    }

    @Test
    public void testConcurrentUpdate() throws Exception {
        ReplicaConnection replicaConnection0 = null;
        ReplicaConnection replicaConnection1 = null;

        try {
            replicaConnection0 = connectionFactory.get(0, 0);
            replicaConnection0.lastSessionInfo();
            replicaConnection0.setLowWaterMark(-1L);

            ArrayList<Record> records;

            int transactionId = 0;
            records = makeRecords(transactionId, transactionId + 10);
            replicaConnection0.appendRecords(records);
            transactionId += records.size();

            // open a new connection
            replicaConnection1 = connectionFactory.get(0, 1);

            // bump up the current session id and makes replicaConnection0 unusable.
            replicaConnection1.lastSessionInfo();

            records = makeRecords(transactionId, transactionId + 10);
            try {
                replicaConnection0.appendRecords(records);
                fail();

            } catch (StorageRpcException ex) {
                // OK
            }

            replicaConnection1.setLowWaterMark(transactionId - 1);
            replicaConnection1.appendRecords(records);
            transactionId += records.size();
            assertEquals(transactionId - 1L, replicaConnection1.getMaxTransactionId());

        } finally {
            if (replicaConnection0 != null) {
                replicaConnection0.close();
            }

            if (replicaConnection1 != null) {
                replicaConnection1.close();
            }
        }
    }

    @Test
    public void testTruncate() throws Exception {
        final ArrayList<Record> records = makeRecords(0, 20);
        final int transactionId = records.size();

        try (ReplicaConnection replicaConnection0 = connectionFactory.get(0, 0)) {
            replicaConnection0.lastSessionInfo();
            replicaConnection0.setLowWaterMark(-1L);
            replicaConnection0.appendRecords(records);

            assertEquals(records.size() - 1, replicaConnection0.getMaxTransactionId());
        }

        try (ReplicaConnection replicaConnection1 = connectionFactory.get(0, 1)) {
            replicaConnection1.lastSessionInfo();

            long newMaxAfterTruncate = transactionId / 2;
            assertEquals(newMaxAfterTruncate, replicaConnection1.truncate(newMaxAfterTruncate));
            replicaConnection1.setLowWaterMark(newMaxAfterTruncate);

            assertEquals(newMaxAfterTruncate, replicaConnection1.getMaxTransactionId());

            for (Record record : records) {
                Record returnedRecord = replicaConnection1.getRecord(record.transactionId);

                if (record.transactionId <= newMaxAfterTruncate) {
                    assertNotNull(returnedRecord);
                    assertEquals(record.transactionId, returnedRecord.transactionId);
                    assertEquals(record.reqId, returnedRecord.reqId);
                    assertEquals(record.header, returnedRecord.header);
                    assertArrayEquals(record.data, returnedRecord.data);
                    assertEquals(record.checksum, returnedRecord.checksum);
                } else {
                    assertNull(returnedRecord);
                }
            }
        }
    }

    @Test
    public void testSetLowWaterMark() throws Exception {
        int transactionId = 0;

        try (ReplicaConnection replicaConnection0 = connectionFactory.get(0, 0)) {
            SessionInfo sessionInfo = replicaConnection0.lastSessionInfo();
            assertEquals(-1L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            replicaConnection0.setLowWaterMark(-1L);

            ArrayList<Record> records = makeRecords(transactionId, transactionId + 10);
            transactionId += records.size();
            replicaConnection0.appendRecords(records);

            assertEquals(records.size() - 1, replicaConnection0.getMaxTransactionId());
        }

        try (ReplicaConnection replicaConnection1 = connectionFactory.get(0, 1)) {
            SessionInfo sessionInfo = replicaConnection1.lastSessionInfo();
            assertEquals(0L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);
            replicaConnection1.setLowWaterMark(transactionId + 10); // exceeding the max transaction id
        }

        try (ReplicaConnection replicaConnection2 = connectionFactory.get(0, 2)) {
            SessionInfo sessionInfo = replicaConnection2.lastSessionInfo();
            assertEquals(1L, sessionInfo.sessionId);
            assertEquals(transactionId + 10, sessionInfo.lowWaterMark);
        }
    }

    @Test
    public void testMultipleSegments() throws Exception {
        final Path dir = helper.getWaltzStorageRunner().directory().resolve("0");
        final long numRecords = 100;

        int count1 = 0;
        int count2 = 0;
        int count3 = 0;

        try (ReplicaConnection replicaConnection0 = connectionFactory.get(0, 0)) {
            replicaConnection0.lastSessionInfo();
            replicaConnection0.setLowWaterMark(-1L);

            replicaConnection0.appendRecords(makeRecords(0, 0 + numRecords));

            for (Path file : Files.newDirectoryStream(dir, "*.seg")) {
                count1++;
            }
            assertTrue(count1 > 1);
        }

        try (ReplicaConnection replicaConnection1 = connectionFactory.get(0, 0)) {
            replicaConnection1.lastSessionInfo();
            assertEquals(numRecords / 2, replicaConnection1.truncate(numRecords / 2));

            assertEquals(numRecords / 2, replicaConnection1.getMaxTransactionId());
            for (int i = 0; i <= numRecords / 2; i++) {
                assertNotNull(replicaConnection1.getRecord(i));
            }
            assertNull(replicaConnection1.getRecord(numRecords / 2 + 1));

            for (Path file : Files.newDirectoryStream(dir, "*.seg")) {
                count2++;
            }
            assertTrue(count2 > 0 && count2 < count1);

            assertEquals(-1L, replicaConnection1.truncate(-1L));

            assertEquals(-1L, replicaConnection1.getMaxTransactionId());
            assertNull(replicaConnection1.getRecord(0));

            for (Path file : Files.newDirectoryStream(dir, "*.seg")) {
                count3++;
            }
            assertEquals(1, count3);
        }
    }

    private ArrayList<Record> makeRecords(long startTransactionId, long endTransactionId) {
        ArrayList<Record> records = new ArrayList<>();
        long transactionId = startTransactionId;
        while (transactionId < endTransactionId) {
            Record record = TestUtils.record(transactionId++);
            records.add(record);
        }
        return records;
    }

}
