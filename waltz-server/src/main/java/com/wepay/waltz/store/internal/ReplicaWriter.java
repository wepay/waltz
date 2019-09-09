package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.store.exception.ReplicaConnectionException;
import com.wepay.waltz.store.exception.ReplicaWriterException;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This class handles the writes to the replicas.
 */
public class ReplicaWriter {

    private final CompletableFuture<ReplicaConnection> connectionFuture;

    private long nextTransactionId = -1L;
    private boolean running = true;

    /**
     * Class constructor.
     * @param connectionFuture The completable future of {@link ReplicaConnection}.
     */
    public ReplicaWriter(CompletableFuture<ReplicaConnection> connectionFuture) {
        this.connectionFuture = connectionFuture;
    }

    /**
     * Opens the replica writer for the given transaction Id.
     * @param nextTransactionId The transaction Id.
     */
    public void open(long nextTransactionId) {
        synchronized (this) {
            this.nextTransactionId = nextTransactionId;
        }
    }

    /**
     * Appends the given {@link StoreAppendRequest}s.
     * @param transactionId The transaction Id.
     * @param requests The {@link StoreAppendRequest}s.
     * @throws ReplicaWriterException thrown if the write fails.
     */
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

    /**
     * Appends the given list of {@link Record}s.
     * @param records The list of {@code Record}s.
     * @throws ReplicaWriterException thrown if the write fails.
     */
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

    /**
     * Returns the next transaction Id.
     * @return the next transaction Id.
     * @throws ReplicaWriterException thrown if the write fails.
     */
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
