package com.wepay.waltz.storage.client;

import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.message.Record;
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
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class StorageAdminClientTest {

    private static final long SESSION_ID = 0;
    private static final int NUM_PARTITIONS = 2;
    private static final long segmentSizeThreshold = 400L;
    private static final String workDirName = "storage-admin-client-test";

    private SslContext sslCtx;
    private UUID key;
    private String host;
    private PortFinder portFinder;
    private Path storageDir;
    private WaltzStorageConfig waltzStorageConfig;

    @Before
    public void setup() throws Exception {
        sslCtx = ClientSSL.createInsecureContext();

        key = UUID.randomUUID();

        host = InetAddress.getLocalHost().getCanonicalHostName();

        portFinder = new PortFinder();
        int storageJettyPort = portFinder.getPort();

        storageDir = Files.createTempDirectory(workDirName).resolve("storage-" + storageJettyPort);
        if (!Files.exists(storageDir)) {
            Files.createDirectory(storageDir);
        }

        Properties storageProps = new Properties();
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
    public void testSetAvailableCanRead() throws Exception {
        SslContext sslCtx = ClientSSL.createInsecureContext();
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

            ArrayList<Record> records = ClientUtil.makeRecords(0, 10);
            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.setLowWaterMark(SESSION_ID, 0, -1L).get();
            client.appendRecords(SESSION_ID, 0, records).get();
            Record record = records.get(0);
            Record returnedRecord = (Record) client.getRecord(SESSION_ID, 0, record.transactionId).get();
            assertEquals(record.transactionId, returnedRecord.transactionId);
            assertEquals(record.reqId, returnedRecord.reqId);
            assertEquals(record.header, returnedRecord.header);
            assertArrayEquals(record.data, returnedRecord.data);
            assertEquals(record.checksum, returnedRecord.checksum);
            adminClient.setPartitionAvailable(0, false).get();
            try {
                client.getRecord(SESSION_ID, 0, records.get(0).transactionId).get();
                fail();
            } catch (ExecutionException ex) {
                assertEquals(StorageRpcException.class, ex.getCause().getClass());
                assertEquals("com.wepay.waltz.storage.exception.StorageException: read and write for partition=0 have been disabled on storage node", ex.getCause().getMessage());
                // OK
            }
            adminClient.close();
            client.close();
        } finally {
            storageRunner.stop();
        }
    }

    @Test
    public void testSetAvailableCanWrite() throws Exception {
        SslContext sslCtx = ClientSSL.createInsecureContext();
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

            ArrayList<Record> records = ClientUtil.makeRecords(0, 10);
            ArrayList<Record> records2 = ClientUtil.makeRecords(10, 20);
            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.setLowWaterMark(SESSION_ID, 0, -1L).get();
            client.appendRecords(SESSION_ID, 0, records).get();
            adminClient.setPartitionAvailable(0, false).get();
            try {
                client.appendRecords(SESSION_ID, 0, records2).get();
                fail();
            } catch (ExecutionException ex) {
                assertEquals(StorageRpcException.class, ex.getCause().getClass());
                assertEquals("com.wepay.waltz.storage.exception.StorageException: read and write for partition=0 have been disabled on storage node", ex.getCause().getMessage());
                // OK
            }
            adminClient.close();
            client.close();
        } finally {
            storageRunner.stop();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetRecordList() throws Exception {
        SslContext sslCtx = ClientSSL.createInsecureContext();
        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            // Init storage node.
            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            // Write records.
            ArrayList<Record> records = ClientUtil.makeRecords(0, 10);
            int numRecords = records.size();
            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.appendRecords(SESSION_ID, 0, records).get();
            client.setLowWaterMark(SESSION_ID, 0, numRecords - 1).get();
            client.close();

            // Get records.
            ArrayList<Record> oneRecordList = (ArrayList<Record>) adminClient.getRecordList(0, 0, 1).get();
            ArrayList<Record> nineRecordList = (ArrayList<Record>) adminClient.getRecordList(0, 1, numRecords - 1).get();
            adminClient.close();

            // Verify records.
            assertEquals(1, oneRecordList.size());
            assertEquals(records.get(0), oneRecordList.get(0));
            assertEquals(9, nineRecordList.size());
            for (int i = 1; i < records.size(); ++i) {
                assertEquals(records.get(i), nineRecordList.get(i - 1));
            }
        } finally {
            storageRunner.stop();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLastSessionInfo() throws Exception {
        long newSessionId = SESSION_ID + 1L;
        WaltzStorageRunner storageRunner = new WaltzStorageRunner(portFinder, waltzStorageConfig, segmentSizeThreshold);
        try {
            storageRunner.startAsync();
            WaltzStorage waltzStorage = storageRunner.awaitStart();

            // Init storage node.
            int adminPort = waltzStorage.adminPort;
            StorageAdminClient adminClient = new StorageAdminClient(host, adminPort, sslCtx, key, NUM_PARTITIONS);
            adminClient.open();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                adminClient.setPartitionAssignment(i, true, false).get();
            }

            // Write records.
            ArrayList<Record> records = ClientUtil.makeRecords(0, 10);
            int numRecords = records.size();
            int port = waltzStorage.port;
            StorageClient client = new StorageClient(host, port, sslCtx, key, NUM_PARTITIONS);
            client.open();
            client.appendRecords(SESSION_ID, 1, records).get();
            client.setLowWaterMark(newSessionId, 1, numRecords - 1).get();
            client.close();

            // Get last session info.
            SessionInfo sessionInfo = (SessionInfo) adminClient.lastSessionInfo(1).get();
            adminClient.close();

            // Verify last session info.
            assertEquals(newSessionId, sessionInfo.sessionId);
            assertEquals(numRecords - 1, sessionInfo.lowWaterMark);
        } finally {
            storageRunner.stop();
        }
    }

}
