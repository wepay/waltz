package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.exception.ReplicaConnectionException;
import com.wepay.waltz.store.exception.ReplicaReaderException;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This class is used to read metadata from replica.
 */
public class ReplicaReader {

    private CompletableFuture<ReplicaConnection> connectionFuture;

    /**
     * Class constructor.
     * @param connectionFuture The completable future of {@link ReplicaConnection}.
     */
    public ReplicaReader(CompletableFuture<ReplicaConnection> connectionFuture) {
        this.connectionFuture = connectionFuture;
    }

    /**
     * Returns the record of the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link Record}.
     * @throws ReplicaReaderException thrown if Storage connection fails.
     */
    public Record getRecord(long transactionId) throws ReplicaReaderException {
        try {
            return getConnection().getRecord(transactionId);

        } catch (ReplicaConnectionException | StorageRpcException ex) {
            throw new ReplicaReaderException("error getting transaction record", ex);
        }
    }

    /**
     * Returns {@link Record}s for the given list of transaction Ids.
     * @param transactionId The transaction Id.
     * @param maxNumRecords The maximum number of records.
     * @return list of {@link Record}s.
     * @throws ReplicaReaderException thrown if it fails to read the metadata from replica.
     */
    public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws ReplicaReaderException {
        try {
            return getConnection().getRecordList(transactionId, maxNumRecords);

        } catch (ReplicaConnectionException | StorageRpcException ex) {
            throw new ReplicaReaderException("error getting transaction records", ex);
        }
    }

    /**
     * Returns {@link RecordHeader} for the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link RecordHeader}.
     * @throws ReplicaReaderException thrown if it fails to read the metadata from replica.
     */
    public RecordHeader getRecordHeader(long transactionId) throws ReplicaReaderException {
        try {
            return getConnection().getRecordHeader(transactionId);

        } catch (ReplicaConnectionException | StorageRpcException ex) {
            throw new ReplicaReaderException("error getting transaction record header", ex);
        }
    }

    /**
     * Returns {@link RecordHeader}s for the given list of transaction Ids.
     * @param transactionId The transaction Id.
     * @param maxNumRecords The maximum number of records.
     * @return list of {@link RecordHeader}s.
     * @throws ReplicaReaderException thrown if it fails to read the metadata from replica.
     */
    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws ReplicaReaderException {
        try {
            return getConnection().getRecordHeaderList(transactionId, maxNumRecords);

        } catch (ReplicaConnectionException | StorageRpcException ex) {
            throw new ReplicaReaderException("error getting transaction records", ex);
        }
    }

    private ReplicaConnection getConnection() throws ReplicaConnectionException {
        while (true) {
            try {
                return connectionFuture.get();

            } catch (InterruptedException ex) {
                Thread.interrupted();
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof ReplicaConnectionException) {
                    throw (ReplicaConnectionException) cause;
                }
                throw new ReplicaConnectionException("failed to connect", ex);
            }
        }
    }

}
