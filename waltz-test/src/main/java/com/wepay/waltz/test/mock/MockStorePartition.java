package com.wepay.waltz.test.mock;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.store.StorePartition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;

public class MockStorePartition implements StorePartition {

    private final int partitionId;
    private final ConcurrentHashMap<Long, Record> records = new ConcurrentHashMap<>();
    private long highWaterMark = -1L;

    public MockStorePartition(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public int partitionId() {
        return partitionId;
    }

    @Override
    public void generation(int generation) {
        // Do nothing
    }

    @Override
    public int generation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHealthy() {
        return true;
    }

    @Override
    public void append(ReqId reqId, int header, byte[] data, int checksum, LongConsumer onCompletion) {
        onCompletion.accept(put(reqId, header, data, checksum));
    }

    @Override
    public RecordHeader getRecordHeader(long transactionId) {
        Record record = get(transactionId);

        if (record == null) {
            throw new IllegalStateException(
                "failed to find transaction: partitionId=" + partitionId + " transactionId=" + transactionId
            );
        }

        return new RecordHeader(transactionId, record.reqId, record.header);
    }

    @Override
    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) {
        ArrayList<RecordHeader> list = new ArrayList<>();

        for (int i = 0; i < maxNumRecords; i++) {
            Record record = get(transactionId);

            if (record == null) {
                break;
            } else {
                list.add(new RecordHeader(transactionId, record.reqId, record.header));
            }
        }

        if (list.isEmpty()) {
            throw new IllegalStateException(
                "failed to find transaction: partitionId=" + partitionId + " transactionId=" + transactionId
            );
        }

        return list;
    }

    @Override
    public Record getRecord(long transactionId) {
        return get(transactionId);
    }

    @Override
    public long highWaterMark() {
        synchronized (this) {
            return highWaterMark;
        }
    }

    @Override
    public int numPendingAppends() {
        return 0;
    }

    @Override
    public long flush() {
        return highWaterMark();
    }

    public Record get(long transactionId) {
        return records.get(transactionId);
    }

    public List<Record> getAllRecords() {
        int size = records.size();

        List<Record> list = new ArrayList<>();

        for (long i = 0; i < size; i++) {
            Record rec = records.get(i);
            if (rec == null) {
                throw new IllegalStateException("missing transaction id: " + i);
            }
            list.add(rec);
        }

        return list;
    }

    public long put(ReqId reqId, int header, byte[] data, int checksum) {
        synchronized (this) {
            this.notifyAll();

            long transactionId = ++highWaterMark;
            Record existing = records.putIfAbsent(
                transactionId,
                new Record(transactionId, reqId, header, data, checksum)
            );

            if (existing != null) {
                throw new IllegalStateException("transaction already persisted: transactionId=" + transactionId);
            }

            return transactionId;
        }
    }

    public void await(long transactionId, long timeout) throws InterruptedException {
        synchronized (this) {
            long due = System.currentTimeMillis() + timeout;

            while (highWaterMark < transactionId) {
                long remaining = due - System.currentTimeMillis();

                if (remaining > 0) {
                    this.wait(remaining);
                } else {
                    break;
                }
            }
        }
    }

}
