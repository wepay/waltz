package com.wepay.waltz.storage.client;

import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.storage.WaltzStorage;
import com.wepay.waltz.storage.WaltzStorageConfig;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.test.util.ClientUtil;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import io.netty.handler.ssl.SslContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class StorageClientTest {

    private static final long SESSION_ID = 0;
    private static final int NUM_PARTITIONS = 2;
    private static final long segmentSizeThreshold = 400L;
    private static final String workDirName = "storage-client-test";
    private static final Random RANDOM = new Random();

    private SslContext sslCtx;
    private UUID key;
    private String host;
    private PortFinder portFinder;
    private Path storageDir;
    private int storageJettyPort;
    private Properties storageProps;
    private WaltzStorageConfig waltzStorageConfig;

    @Before
    public void setup() throws Exception {
        sslCtx = ClientSSL.createInsecureContext();

        key = UUID.randomUUID();

        host = InetAddress.getLocalHost().getCanonicalHostName();

        portFinder = new PortFinder();
        storageJettyPort = portFinder.getPort();

        storageDir = Files.createTempDirectory(workDirName).resolve("storage-" + storageJettyPort);
        if (!Files.exists(storageDir)) {
            Files.createDirectory(storageDir);
        }

        storageProps = new Properties();
        storageProps.setProperty(WaltzStorageConfig.STORAGE_JETTY_PORT, String.valueOf(storageJettyPort));
        storageProps.setProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD, String.valueOf(segmentSizeThreshold));
        storageProps.setProperty(WaltzStorageConfig.STORAGE_DIRECTORY, storageDir.toString());
        storageProps.setProperty(WaltzStorageConfig.CLUSTER_NUM_PARTITIONS, String.valueOf(NUM_PARTITIONS));
        storageProps.setProperty(WaltzStorageConfig.CLUSTER_KEY, String.valueOf(key));
        waltzStorageConfig = new WaltzStorageConfig(storageProps);
    }

    @After
    public void tearDown() {
        if (storageDir != null) {
            Utils.removeDirectory(storageDir.toFile());
            storageDir = null;
        }
    }

    @Test
    public void testBasicReadWrite() throws Exception {
        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();

            SessionInfo sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID, 0).get();
            assertEquals(-1L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID, 1).get();
            assertEquals(-1L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            client.setLowWaterMark(SESSION_ID, 0, -1L).get();
            client.setLowWaterMark(SESSION_ID, 1, -1L).get();

            ArrayList<Record> allRecords0 = new ArrayList<>();
            ArrayList<Record> allRecords1 = new ArrayList<>();

            int transactionId;

            // Partition 0

            transactionId = 0;
            while (transactionId < 100) {
                int n = RANDOM.nextInt(50);
                ArrayList<Record> records = ClientUtil.makeRecords(transactionId, transactionId + n);
                transactionId += records.size();
                client.appendRecords(SESSION_ID, 0, records).get();
                allRecords0.addAll(records);
            }

            assertEquals((long) allRecords0.size() - 1, client.getMaxTransactionId(SESSION_ID, 0).get());
            assertEquals((long) allRecords1.size() - 1, client.getMaxTransactionId(SESSION_ID, 1).get());

            for (Record record : allRecords0) {
                Record returnedRecord = (Record) client.getRecord(SESSION_ID, 0, record.transactionId).get();

                assertEquals(record.transactionId, returnedRecord.transactionId);
                assertEquals(record.reqId, returnedRecord.reqId);
                assertEquals(record.header, returnedRecord.header);
                assertArrayEquals(record.data, returnedRecord.data);
                assertEquals(record.checksum, returnedRecord.checksum);
            }

            for (Record record : allRecords0) {
                RecordHeader returnedRecordHeader
                    = (RecordHeader) client.getRecordHeader(SESSION_ID, 0, record.transactionId).get();

                assertEquals(record.transactionId, returnedRecordHeader.transactionId);
                assertEquals(record.reqId, returnedRecordHeader.reqId);
                assertEquals(record.header, returnedRecordHeader.header);
            }

            // Partition 1

            transactionId = 0;
            while (transactionId < 100) {
                int n = RANDOM.nextInt(50);
                ArrayList<Record> records = ClientUtil.makeRecords(transactionId, transactionId + n);
                transactionId += records.size();
                client.appendRecords(SESSION_ID, 1, records).get();
                allRecords1.addAll(records);
            }

            assertEquals((long) allRecords0.size() - 1, client.getMaxTransactionId(SESSION_ID, 0).get());
            assertEquals((long) allRecords1.size() - 1, client.getMaxTransactionId(SESSION_ID, 1).get());

            for (Record record : allRecords1) {
                Record returnedRecord = (Record) client.getRecord(SESSION_ID, 1, record.transactionId).get();

                assertEquals(record.transactionId, returnedRecord.transactionId);
                assertEquals(record.reqId, returnedRecord.reqId);
                assertEquals(record.header, returnedRecord.header);
                assertArrayEquals(record.data, returnedRecord.data);
                assertEquals(record.checksum, returnedRecord.checksum);
            }

            for (Record record : allRecords1) {
                RecordHeader returnedRecordHeader
                    = (RecordHeader) client.getRecordHeader(SESSION_ID, 1, record.transactionId).get();

                assertEquals(record.transactionId, returnedRecordHeader.transactionId);
                assertEquals(record.reqId, returnedRecordHeader.reqId);
                assertEquals(record.header, returnedRecordHeader.header);
            }

            sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID, 0).get();
            assertEquals(0L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID, 1).get();
            assertEquals(0L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            client.close();

        } finally {
            storageRunner.stop();
        }
    }

    @Test
    public void testTransactionOutOfOrder() throws Exception {
        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.setLowWaterMark(SESSION_ID, 0, -1L).get();

            int transactionId = 0;
            ArrayList<Record> records = ClientUtil.makeRecords(transactionId, transactionId + 20);
            client.appendRecords(SESSION_ID, 0, records).get();

            // one overlapping transaction id
            transactionId--;
            records = ClientUtil.makeRecords(transactionId, transactionId + 20);

            try {
                client.appendRecords(SESSION_ID, 0, records).get();
                fail();

            } catch (ExecutionException ex) {
                assertTrue(ex.getCause() instanceof StorageRpcException);
                // OK
            }

            client.close();

        } finally {
            storageRunner.stop();
        }
    }

    @Test
    public void testSingleWriterGuarantee() throws Exception {
        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.lastSessionInfo(SESSION_ID, 0).get();
            client.setLowWaterMark(SESSION_ID, 0, -1L).get();

            ArrayList<Record> records;

            int transactionId = 0;
            records = ClientUtil.makeRecords(transactionId, transactionId + 10);
            client.appendRecords(SESSION_ID, 0, records).get();
            transactionId += records.size();

            // start a new session
            // bump up the current session id and makes the previous session unusable.
            client.lastSessionInfo(SESSION_ID + 1, 0);

            records = ClientUtil.makeRecords(transactionId, transactionId + 10);
            try {
                client.appendRecords(SESSION_ID, 0, records).get();
                fail();
            } catch (ExecutionException ex) {
                assertTrue(ex.getCause() instanceof StorageRpcException);
            }

            client.setLowWaterMark(SESSION_ID + 1, 0, transactionId - 1).get();
            client.appendRecords(SESSION_ID + 1, 0, records).get();
            transactionId += records.size();
            assertEquals(transactionId - 1L, client.getMaxTransactionId(SESSION_ID + 1, 0).get());

            client.close();

        } finally {
            storageRunner.stop();
        }
    }

    @Test
    public void testTruncate() throws Exception {
        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.lastSessionInfo(SESSION_ID, 0).get();
            client.setLowWaterMark(SESSION_ID, 0, -1L).get();

            ArrayList<Record> records;
            int transactionId;

            transactionId = 0;
            records = ClientUtil.makeRecords(transactionId, transactionId + 20);
            transactionId += records.size();
            client.appendRecords(SESSION_ID, 0, records).get();

            assertEquals((long) (records.size() - 1), client.getMaxTransactionId(SESSION_ID, 0).get());

            client.lastSessionInfo(SESSION_ID + 1, 0);

            long newMaxAfterTruncate = transactionId / 2;
            assertEquals(newMaxAfterTruncate, (long) client.truncate(SESSION_ID + 1, 0, newMaxAfterTruncate).get());
            client.setLowWaterMark(SESSION_ID + 1, 0, newMaxAfterTruncate).get();

            assertEquals(newMaxAfterTruncate, client.getMaxTransactionId(SESSION_ID + 1, 0).get());

            for (Record record : records) {
                Record returnedRecord = (Record) client.getRecord(SESSION_ID + 1, 0, record.transactionId).get();

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

            client.close();

        } finally {
            storageRunner.stop();
        }
    }

    @Test
    public void testSetLowWaterMark() throws Exception {
        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();

            int transactionId;
            ArrayList<Record> records;

            SessionInfo sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID, 0).get();
            assertEquals(-1L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            client.setLowWaterMark(SESSION_ID, 0, -1L).get();

            transactionId = 0;
            records = ClientUtil.makeRecords(transactionId, transactionId + 10);
            transactionId += records.size();
            client.appendRecords(SESSION_ID, 0, records);

            assertEquals((long) (records.size() - 1), client.getMaxTransactionId(SESSION_ID, 0).get());

            sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID, 0).get();
            assertEquals(0L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID + 1, 0).get();
            assertEquals(0L, sessionInfo.sessionId);
            assertEquals(-1L, sessionInfo.lowWaterMark);

            client.setLowWaterMark(SESSION_ID + 1, 0, transactionId + 10); // exceeding the max transaction id

            sessionInfo = (SessionInfo) client.lastSessionInfo(SESSION_ID + 1, 0).get();
            assertEquals(1L, sessionInfo.sessionId);
            assertEquals(transactionId + 10, sessionInfo.lowWaterMark);

            client.close();

        } finally {
            storageRunner.stop();
        }
    }

    @Test
    public void testMultipleSegments() throws Exception {
        final long numRecords = 100;

        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.lastSessionInfo(SESSION_ID, 0).get();
            client.setLowWaterMark(SESSION_ID, 0, -1L).get();

            client.appendRecords(SESSION_ID, 0, ClientUtil.makeRecords(0, 0 + numRecords)).get();

            Path dir = storageRunner.directory().resolve("0");
            int count1 = 0;
            for (Path file : Files.newDirectoryStream(dir, "*.seg")) {
                count1++;
            }
            assertTrue(count1 > 1);

            client.lastSessionInfo(SESSION_ID + 1, 0).get();
            assertEquals(numRecords / 2, (long) client.truncate(SESSION_ID + 1, 0, numRecords / 2).get());

            assertEquals(numRecords / 2, (client.getMaxTransactionId(SESSION_ID + 1, 0).get()));
            for (int i = 0; i <= numRecords / 2; i++) {
                assertNotNull(client.getRecord(SESSION_ID + 1, 0, i).get());
            }
            assertNull(client.getRecord(SESSION_ID + 1, 0, numRecords / 2 + 1).get());

            int count2 = 0;
            for (Path file : Files.newDirectoryStream(dir, "*.seg")) {
                count2++;
            }
            assertTrue(count2 > 0 && count2 < count1);

            assertEquals(-1L, (long) client.truncate(SESSION_ID + 1, 0, -1L).get());

            assertEquals(-1L, client.getMaxTransactionId(SESSION_ID + 1, 0).get());
            assertNull(client.getRecord(SESSION_ID + 1, 0, 0).get());

            int count3 = 0;
            for (Path file : Files.newDirectoryStream(dir, "*.seg")) {
                count3++;
            }
            assertEquals(1, count3);
        } finally {
            storageRunner.stop();
        }
    }

}
