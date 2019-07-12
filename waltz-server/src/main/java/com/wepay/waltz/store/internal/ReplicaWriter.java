package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.store.exception.ReplicaConnectionException;
import com.wepay.waltz.store.exception.ReplicaWriterException;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReplicaWriter {

    private final CompletableFuture<ReplicaConnection> connectionFuture;

    private long nextTransactionId = -1L;
    private boolean running = true;

    public ReplicaWriter(CompletableFuture<ReplicaConnection> connectionFuture) {
        this.connectionFuture = connectionFuture;
    }

    public void open(long nextTransactionId) {
        synchronized (this) {
            this.nextTransactionId = nextTransactionId;
        }
    }

    public void append(final long transactionId, final Iterable<StoreAppendRequest> requests) throws ReplicaWriterException {
        synchronized (this) {
            if (!running) {
                throw new ReplicaWriterException("closed");
            }

            if (nextTransactionId != transactionId) {
                running = false;
                throw new ReplicaWriterException("transaction out of order");
            }

            try {
                ArrayList<Record> records = new ArrayList<>();
                long txid = nextTransactionId;
                for (StoreAppendRequest request : requests) {
                    records.add(
                        new Record(txid, request.reqId, request.header, request.data, request.checksum)
                    );
                    txid++;
                }

                ReplicaConnection connection = getConnection();
                connection.appendRecords(records);
                nextTransactionId = txid;

            } catch (Throwable ex) {
                running = false;
                throw new ReplicaWriterException("failed to write", ex);
            }
        }
    }

    public void append(ArrayList<Record> records) throws ReplicaWriterException {
        synchronized (this) {
            if (!running) {
                throw new ReplicaWriterException("closed");
            }

            // Check transaction ids
            long txid = nextTransactionId;
            for (Record record : records) {
                if (record.transactionId != txid) {
                    throw new ReplicaWriterException("transaction out of order");
                }
                txid++;
            }

            try {
                ReplicaConnection connection = getConnection();
                connection.appendRecords(records);
                nextTransactionId = txid;

            } catch (Throwable ex) {
                running = false;
                throw new ReplicaWriterException("failed to write", ex);
            }
        }
    }

    public long nextTransactionId() throws ReplicaWriterException {
        synchronized (this) {
            if (running) {
                return nextTransactionId;
            } else {
                throw new ReplicaWriterException("closed");
            }
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
