package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.io.Closeable;
import java.util.ArrayList;

/**
 * This class handles replica's connection.
 */
public interface ReplicaConnection extends Closeable {

    /**
     * Returns the last {@link SessionInfo}.
     * @return the last {@code SessionInfo}.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    SessionInfo lastSessionInfo() throws StorageRpcException;

    /**
     * Sets the low-water mark.
     * @param lowWaterMark The low-water mark.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    void setLowWaterMark(long lowWaterMark) throws StorageRpcException;

    /**
     * Truncates the transactions for the given transaction id.
     * @param transactionId The transaction Id.
     * @return the high-water mark.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    long truncate(long transactionId) throws StorageRpcException;

    /**
     * Returns {@link RecordHeader} for the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link RecordHeader}.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    RecordHeader getRecordHeader(long transactionId) throws StorageRpcException;

    /**
     * Returns {@link RecordHeader}s for the given list of transaction Ids.
     * @param transactionId The transaction Id.
     * @param maxNumRecords The maximum number of records.
     * @return list of {@link RecordHeader}s.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StorageRpcException;

    /**
     * Returns {@link Record} for the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link Record}.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    Record getRecord(long transactionId) throws StorageRpcException;

    /**
     * Returns {@link Record}s for the given list of transaction Ids.
     * @param transactionId The transaction Id.
     * @param maxNumRecords The maximum number of records.
     * @return list of {@link Record}s.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws StorageRpcException;

    /**
     * Returns the maximum transaction Id.
     * @return maximum transaction Id.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    long getMaxTransactionId() throws StorageRpcException;

    /**
     * Returns the partition Id.
     * @return the partition Id.
     */
    int getPartitionId();

    /**
     * Appends the given list of {@link Record}s.
     * @param records List of {@code Record}s.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    void appendRecords(ArrayList<Record> records) throws StorageRpcException;

    /**
     * Closes the Replica connection.
     */
    void close();

}
