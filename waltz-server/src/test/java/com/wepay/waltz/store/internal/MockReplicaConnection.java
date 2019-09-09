package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.util.ArrayList;

class MockReplicaConnection implements ReplicaConnection {

    private final int partitionId;
    private final long sessionId;
    private final int seqNum;
    private final MockReplicaConnectionFactory factory;

    MockReplicaConnection(int partitionId, long sessionId, int seqNum, MockReplicaConnectionFactory factory) {
        this.partitionId = partitionId;
        this.sessionId = sessionId;
        this.seqNum = seqNum;
        this.factory = factory;
    }

    @Override
    public SessionInfo lastSessionInfo() throws StorageRpcException {
        factory.isUp(seqNum);
        return factory.getLastSessionInfo(partitionId, sessionId);
    }

    @Override
    public void setLowWaterMark(long lowWaterMark) throws StorageRpcException {
        factory.isUp(seqNum);
        factory.setLastSessionInfo(partitionId, sessionId, lowWaterMark);
    }

    @Override
    public long truncate(long transactionId) throws StorageRpcException {
        factory.isUp(seqNum);
        return factory.truncate(partitionId, sessionId, transactionId);
    }

    @Override
    public RecordHeader getRecordHeader(long transactionId) throws StorageRpcException {
        factory.isUp(seqNum);
        Record record = getRecord(transactionId);
        if (record != null) {
            return new RecordHeader(record.transactionId, record.reqId, record.header);
        } else {
            return null;
        }
    }

    @Override
    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StorageRpcException {
        factory.isUp(seqNum);

        ArrayList<RecordHeader> list = new ArrayList<>();
        for (int i = 0; i < maxNumRecords; i++) {
            Record record = factory.getRecord(partitionId, sessionId, transactionId + 1);
            if (record == null) {
                break;
            } else {
                list.add(new RecordHeader(record.transactionId, record.reqId, record.header));
            }
        }
        return list;
    }

    @Override
    public Record getRecord(long transactionId) throws StorageRpcException {
        factory.isUp(seqNum);
        return factory.getRecord(partitionId, sessionId, transactionId);
    }

    @Override
    public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws StorageRpcException {
        factory.isUp(seqNum);

        ArrayList<Record> list = new ArrayList<>();
        for (int i = 0; i < maxNumRecords; i++) {
            Record record = factory.getRecord(partitionId, sessionId, transactionId + i);
            if (record == null) {
                break;
            } else {
                list.add(record);
            }
        }
        return list;
    }

    @Override
    public long getMaxTransactionId() throws StorageRpcException {
        factory.isUp(seqNum);
        return factory.getMaxTransactionId(partitionId, sessionId);
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void appendRecords(ArrayList<Record> records) throws StorageRpcException {
        factory.isUp(seqNum);
        factory.appendRecords(partitionId, sessionId, records);
    }

    @Override
    public void close() {

    }

}
