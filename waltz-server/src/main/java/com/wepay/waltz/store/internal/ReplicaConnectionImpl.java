package com.wepay.waltz.store.internal;

import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReplicaConnectionImpl implements ReplicaConnection {

    private final int partitionId;
    private final long sessionId;
    private final StorageClient client;
    private final SessionInfo lastSessionInfo;

    public ReplicaConnectionImpl(int partitionId, long sessionId, StorageClient client) throws StorageRpcException {
        this.partitionId = partitionId;
        this.sessionId = sessionId;
        this.client = client;
        this.lastSessionInfo = (SessionInfo) get(client.lastSessionInfo(sessionId, partitionId));
    }

    public SessionInfo lastSessionInfo() {
        return lastSessionInfo;
    }

    public void setLowWaterMark(long lowWaterMark) throws StorageRpcException {
        get(client.setLowWaterMark(sessionId, partitionId, lowWaterMark));
    }

    public long truncate(long transactionId) throws StorageRpcException {
        return (Long) get(client.truncate(sessionId, partitionId, transactionId));
    }

    public RecordHeader getRecordHeader(long transactionId) throws StorageRpcException {
        return (RecordHeader) get(client.getRecordHeader(sessionId, partitionId, transactionId));
    }

    @SuppressWarnings("unchecked")
    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StorageRpcException {
        return (ArrayList<RecordHeader>) get(client.getRecordHeaderList(sessionId, partitionId, transactionId, maxNumRecords));
    }

    public Record getRecord(long transactionId) throws StorageRpcException {
        return (Record) get(client.getRecord(sessionId, partitionId, transactionId));
    }

    @SuppressWarnings("unchecked")
    public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws StorageRpcException {
        return (ArrayList<Record>) get(client.getRecordList(sessionId, partitionId, transactionId, maxNumRecords));
    }

    public long getMaxTransactionId() throws StorageRpcException {
        return (Long) get(client.getMaxTransactionId(sessionId, partitionId));
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    public void appendRecords(ArrayList<Record> records) throws StorageRpcException {
        get(client.appendRecords(sessionId, partitionId, records));
    }

    public void close() {
    }

    private Object get(CompletableFuture<Object> future) throws StorageRpcException {
        while (true) {
            try {
                return future.get();

            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof StorageRpcException) {
                    throw (StorageRpcException) cause;
                } else {
                    throw new StorageRpcException(cause);
                }

            } catch (InterruptedException ignore) {
                Thread.interrupted();
            }
        }
    }

}
