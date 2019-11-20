package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.store.TestUtils;
import com.wepay.waltz.common.metadata.store.internal.ReplicaId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReplicaReaderTest {
    private static final int PARTITION_ID = 0;
    private static final int NUM_PARTITIONS = 1;
    private static final long SESSION_ID = 999;

    @Test
    public void testGetRecord() throws Exception {
        ReplicaId replicaId = new ReplicaId(PARTITION_ID, String.format(TestReplicaSessionManager.CONNECT_STRING_TEMPLATE, 0));
        MockReplicaConnectionFactory connectionFactory = new MockReplicaConnectionFactory(NUM_PARTITIONS);

        try {
            ArrayList<Record> records = makeRecords(0, 10);
            connectionFactory.setCurrentSession(PARTITION_ID, SESSION_ID);
            connectionFactory.appendRecords(PARTITION_ID, SESSION_ID, records);

            CompletableFuture<ReplicaConnection> future = new CompletableFuture<>();
            future.complete(connectionFactory.get(replicaId.partitionId, SESSION_ID));

            ReplicaReader reader = new ReplicaReader(future);

            for (Record r : records) {
                RecordHeader recordHeader = reader.getRecordHeader(r.transactionId);

                assertEquals(r.transactionId, recordHeader.transactionId);
                assertEquals(r.reqId, recordHeader.reqId);
                assertEquals(r.header, recordHeader.header);
            }

            assertNull(reader.getRecordHeader(11));

            for (Record r : records) {
                Record record = reader.getRecord(r.transactionId);

                assertEquals(r.reqId, record.reqId);
                assertEquals(r.header, record.header);
                assertTrue(Arrays.equals(r.data, record.data));
                assertEquals(r.checksum, record.checksum);
            }

            assertNull(reader.getRecord(11));

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
