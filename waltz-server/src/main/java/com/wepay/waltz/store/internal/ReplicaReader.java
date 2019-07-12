package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.exception.ReplicaConnectionException;
import com.wepay.waltz.store.exception.ReplicaReaderException;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReplicaReader {

    private CompletableFuture<ReplicaConnection> connectionFuture;

    public ReplicaReader(CompletableFuture<ReplicaConnection> connectionFuture) {
        this.connectionFuture = connectionFuture;
    }

    public Record getRecord(long transactionId) throws ReplicaReaderException {
        try {
            return getConnection().getRecord(transactionId);

        } catch (ReplicaConnectionException | StorageRpcException ex) {
            throw new ReplicaReaderException("error getting transaction record", ex);
        }
    }

    public ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws ReplicaReaderException {
        try {
            return getConnection().getRecordList(transactionId, maxNumRecords);

        } catch (ReplicaConnectionException | StorageRpcException ex) {
            throw new ReplicaReaderException("error getting transaction records", ex);
        }
    }

    public RecordHeader getRecordHeader(long transactionId) throws ReplicaReaderException {
        try {
            return getConnection().getRecordHeader(transactionId);

        } catch (ReplicaConnectionException | StorageRpcException ex) {
            throw new ReplicaReaderException("error getting transaction record header", ex);
        }
    }

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
