package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.store.exception.ReplicaWriterException;
import com.wepay.waltz.store.internal.metadata.ReplicaId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReplicaWriterTest {
    private static final int PARTITION_ID = 0;
    private static final int NUM_PARTITIONS = 1;
    private static final long SESSION_ID = 999;

    @Test
    public void testTransactionOutOfOrder1() throws Exception {
        ReplicaId replicaId = new ReplicaId(PARTITION_ID, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, 0));
        MockReplicaConnectionFactory connectionFactory = new MockReplicaConnectionFactory(NUM_PARTITIONS);
        connectionFactory.setCurrentSession(PARTITION_ID, SESSION_ID);

        try {
            CompletableFuture<ReplicaConnection> future = new CompletableFuture<>();
            future.complete(connectionFactory.get(replicaId.partitionId, SESSION_ID));

            ReplicaWriter writer = new ReplicaWriter(future);
            writer.open(0L);

            writer.append(0, TestUtils.makeStoreAppendRequests(0L, 10L));

            assertEquals(-1L, connectionFactory.getLowWaterMark(PARTITION_ID, SESSION_ID));
            assertEquals(9L, connectionFactory.getMaxTransactionId(PARTITION_ID, SESSION_ID));
            assertEquals(10L, writer.nextTransactionId());


            writer.append(10L, TestUtils.makeStoreAppendRequests(10L, 20L));

            assertEquals(-1L, connectionFactory.getLowWaterMark(PARTITION_ID, SESSION_ID));
            assertEquals(19L, connectionFactory.getMaxTransactionId(PARTITION_ID, SESSION_ID));
            assertEquals(20L, writer.nextTransactionId());

            try {
                // overlapping transaction id
                writer.append(19L, TestUtils.makeStoreAppendRequests(19L, 30L));
                fail();
            } catch (ReplicaWriterException ex) {
                // OK
            }
            assertEquals(-1L, connectionFactory.getLowWaterMark(PARTITION_ID, SESSION_ID));
            assertEquals(19L, connectionFactory.getMaxTransactionId(PARTITION_ID, SESSION_ID));

        } finally {
            connectionFactory.close();
        }
    }


    @Test
    public void testTransactionOutOfOrder2() throws Exception {
        ReplicaId replicaId = new ReplicaId(PARTITION_ID, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, 0));
        MockReplicaConnectionFactory connectionFactory = new MockReplicaConnectionFactory(NUM_PARTITIONS);
        connectionFactory.setCurrentSession(PARTITION_ID, SESSION_ID);

        try {
            CompletableFuture<ReplicaConnection> future = new CompletableFuture<>();
            future.complete(connectionFactory.get(replicaId.partitionId, SESSION_ID));

            ReplicaWriter writer = new ReplicaWriter(future);
            writer.open(0L);

            writer.append(makeRecords(0L, 10L));

            assertEquals(-1L, connectionFactory.getLowWaterMark(PARTITION_ID, SESSION_ID));
            assertEquals(9L, connectionFactory.getMaxTransactionId(PARTITION_ID, SESSION_ID));
            assertEquals(10L, writer.nextTransactionId());

            writer.append(makeRecords(10L, 20L));

            assertEquals(-1L, connectionFactory.getLowWaterMark(PARTITION_ID, SESSION_ID));
            assertEquals(19L, connectionFactory.getMaxTransactionId(PARTITION_ID, SESSION_ID));
            assertEquals(20L, writer.nextTransactionId());

            try {
                // overlapping transaction id
                writer.append(makeRecords(19L, 30L));
                fail();
            } catch (ReplicaWriterException ex) {
                // OK
            }
            assertEquals(-1L, connectionFactory.getLowWaterMark(PARTITION_ID, SESSION_ID));
            assertEquals(19L, connectionFactory.getMaxTransactionId(PARTITION_ID, SESSION_ID));

        } finally {
            connectionFactory.close();
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
