package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.io.Closeable;
import java.util.ArrayList;

public interface ReplicaConnection extends Closeable {

    SessionInfo lastSessionInfo() throws StorageRpcException;

    void setLowWaterMark(long lowWaterMark) throws StorageRpcException;

    long truncate(long transactionId) throws StorageRpcException;

    RecordHeader getRecordHeader(long transactionId) throws StorageRpcException;

    ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StorageRpcException;

    Record getRecord(long transactionId) throws StorageRpcException;

    ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws StorageRpcException;

    long getMaxTransactionId() throws StorageRpcException;

    int getPartitionId();

    void appendRecords(ArrayList<Record> records) throws StorageRpcException;

    void close();

}
