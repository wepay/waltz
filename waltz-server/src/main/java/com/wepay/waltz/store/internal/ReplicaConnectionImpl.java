package com.wepay.waltz.store.internal;

import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Implements {@link ReplicaConnection}.
 */
public class ReplicaConnectionImpl implements ReplicaConnection {

    private final int partitionId;
    private final long sessionId;
    private final StorageClient client;
    private final SessionInfo lastSessionInfo;

    /**
     * Class constructor.
     * @param partitionId The partition Id.
     * @param sessionId The session Id.
     * @param client The {@link StorageClient}.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    public ReplicaConnectionImpl(int partitionId, long sessionId, StorageClient client) throws StorageRpcException {
        this.partitionId = partitionId;
        this.sessionId = sessionId;
        this.client = client;
        this.lastSessionInfo = (SessionInfo) get(client.lastSessionInfo(sessionId, partitionId));
    }

    /**
     * Returns the last {@link SessionInfo}.
     * @return the last {@code SessionInfo}.
     */
    public SessionInfo lastSessionInfo() {
        return lastSessionInfo;
    }

    /**
     * Sets the low-water mark.
     * @param lowWaterMark The low-water mark.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    public void setLowWaterMark(long lowWaterMark) throws StorageRpcException {
        get(client.setLowWaterMark(sessionId, partitionId, lowWaterMark));
    }

    /**
     * Truncates the transactions for the given transaction id.
     * @param transactionId The transaction Id.
     * @return the high-water mark.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    public long truncate(long transactionId) throws StorageRpcException {
        return (Long) get(client.truncate(sessionId, partitionId, transactionId));
    }

    /**
     * Returns {@link RecordHeader} for the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link RecordHeader}.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    public RecordHeader getRecordHeader(long transactionId) throws StorageRpcException {
        return (RecordHeader) get(client.getRecordHeader(sessionId, partitionId, transactionId));
    }

    /**
     * Returns {@link RecordHeader}s for the given list of transaction Ids.
     * @param transactionId The transaction Id.
     * @param maxNumRecords The maximum number of records.
     * @return list of {@link RecordHeader}s.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    @SuppressWarnings("unchecked")
    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StorageRpcException {
        return (ArrayList<RecordHeader>) get(client.getRecordHeaderList(sessionId, partitionId, transactionId, maxNumRecords));
    }

    /**
     * Returns {@link Record} for the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link Record}.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    public Record getRecord(long transactionId) throws StorageRpcException {
        return (Record) get(client.getRecord(sessionId, partitionId, transactionId));
    }

    /**
     * Returns {@link Record}s for the given list of transaction Ids.
     * @param transactionId The transaction Id.
     * @param maxNumRecords The maximum number of records.
     * @return list of {@link Record}s.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    @SuppressWarnings("unchecked")
    public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws StorageRpcException {
        return (ArrayList<Record>) get(client.getRecordList(sessionId, partitionId, transactionId, maxNumRecords));
    }

    /**
     * Returns the maximum transaction Id.
     * @return maximum transaction Id.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    public long getMaxTransactionId() throws StorageRpcException {
        return (Long) get(client.getMaxTransactionId(sessionId, partitionId));
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Appends the given list of {@link Record}s.
     * @param records List of {@code Record}s.
     * @throws StorageRpcException thrown if Storage connection fails.
     */
    public void appendRecords(ArrayList<Record> records) throws StorageRpcException {
        get(client.appendRecords(sessionId, partitionId, records));
    }

    /**
     * Closes the Replica connection.
     */
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
