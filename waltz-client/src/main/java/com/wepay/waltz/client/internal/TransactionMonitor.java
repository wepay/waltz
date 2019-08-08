package com.wepay.waltz.client.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.exception.ClientClosedException;
import com.wepay.waltz.exception.PartitionInactiveException;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A class that monitors pending transactions.
 * Each {@link Partition} instance has one {@link TransactionMonitor} instance associated with it.
 */
public class TransactionMonitor {

    private static final Logger logger = Logging.getLogger(TransactionMonitor.class);

    private enum State {
        STARTED, STOPPED, CLOSED
    }

    private final LinkedHashMap<ReqId, TransactionFuture> registered = new LinkedHashMap<>();
    private final int maxConcurrentTransactions;

    private State state = State.STOPPED;
    private ReqId lastReqId = null;
    private long lastTimestamp = -1;
    private int numRegistered = 0;

    /**
     * Class Constructor.
     * @param maxConcurrentTransactions the maximum number of concurrent transactions.
     */
    public TransactionMonitor(int maxConcurrentTransactions) {
        this.maxConcurrentTransactions = maxConcurrentTransactions;
    }

    /**
     * Closes this {@link TransactionMonitor} by completing all pending transactions exceptionally.
     * Sets {@link #state} to {@link State#CLOSED}
     */
    public void close() {
        synchronized (this) {
            state = State.CLOSED;
            if (!registered.isEmpty()) {
                ClientClosedException exception = new ClientClosedException();
                registered.values().forEach(future -> {
                    future.completeExceptionally(exception);
                    future.flushed();
                });
                registered.clear();
                numRegistered = 0;
            }
            notifyAll();
        }
    }

    /**
     * Starts this instance by setting the state to {@link State#STARTED}.
     * Closed transaction monitors cannot be STARTED.
     *
     * @return {@code true}, if started successfully. {@code false}, otherwise.
     */
    public boolean start() {
        synchronized (this) {
            if (state == State.STOPPED) {
                state = State.STARTED;

                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Stops this instance by setting the state to {@link State#STOPPED}
     * Closed transaction monitors cannot be STOPPED.
     *
     * @return {@code true}, if stopped successfully. {@code false}, otherwise.
     */
    public boolean stop() {
        synchronized (this) {
            if (state == State.STARTED) {
                state = State.STOPPED;
                return true;

            } else {
                return false;
            }
        }
    }

    /**
     * @return {@code true}, if this instance is in {@link State#STOPPED} state. {@code false}, otherwise.
     */
    public boolean isStopped() {
        synchronized (this) {
            return state == State.STOPPED;
        }
    }

    /**
     * When this method is called, it is certain that all append requests from this client are flushed,
     * and there shouldn't be any pending transactions.
     * Complete all pending futures by marking them failed.
     */
    public void clear() {
        synchronized (this) {
            registered.values().forEach(future -> {
                future.complete(false);
                future.flushed();
            });
            registered.clear();
            numRegistered = 0;
            notifyAll();
        }
    }

    /**
     * Maximum capacity of this {@code TransactionMonitor} instance i.e., maximum concurrent transactions.
     *
     * @return the maximum number of concurrent transactions (a.k.a maxCapacity)
     */
    public int maxCapacity() {
        return maxConcurrentTransactions;
    }

    /**
     * Registers a transaction represented by {@code reqId}.
     *
     * If the maximum capacity is reached,
     * waits for a maximum of registrationTimeout millis for an existing pending transaction to complete.
     *
     * @param reqId the {@code ReqId} of the transaction.
     * @param registrationTimeout the maximum wait time in millis for registration to complete.
     * @return a {@link TransactionFuture} which will complete when the corresponding transaction is {@link #committed(ReqId)}.
     *         Or, a {@code null}, if {@code registrationTimeout} has passed before registration.
     */
    public TransactionFuture register(ReqId reqId, long registrationTimeout) {
        synchronized (this) {
            final long due = System.currentTimeMillis() + registrationTimeout;
            while (state == State.STARTED && (numRegistered >= maxConcurrentTransactions)) {
                long remaining = due - System.currentTimeMillis();
                if (remaining > 0) {
                    try {
                        wait(remaining);
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                } else {
                    logger.warn("transaction registration timeout: partitionId=" + reqId.partitionId() + " elapsed=" + registrationTimeout + "ms");
                    return null;
                }
            }

            TransactionFuture future = new TransactionFuture(reqId);

            if (state == State.STARTED) {
                if (!registered.containsKey(reqId)) {
                    lastReqId = reqId;
                    lastTimestamp = System.currentTimeMillis();
                    registered.put(reqId, future);
                    numRegistered++;
                    if (numRegistered >= maxConcurrentTransactions) {
                        logger.debug("transaction monitor reached capacity");
                    }
                } else {
                    logger.error("duplicate reqId: reqId=" + reqId);
                    future.complete(false);
                }
            } else {
                if (state == State.STOPPED) {
                    future.completeExceptionally(new PartitionInactiveException(reqId.partitionId()));

                } else {
                    // We are shutting down
                    future.completeExceptionally(new ClientClosedException());
                }
            }

            return future;
        }
    }

    /**
     * If the transaction monitor is not closed,
     * the corresponding {@link TransactionFuture} of the {@code reqId} is completed with {@code true}.
     *
     * @param reqId the {@code ReqId} of the transaction that was committed to a Waltz server.
     */
    public void committed(ReqId reqId) {
        synchronized (this) {
            if (state != State.CLOSED) {
                if (registered.containsKey(reqId)) {
                    // This is own transaction
                    complete(reqId, true);
                    notifyAll();
                }
            }
        }
    }

    /**
     * If the transaction monitor is not closed,
     * the corresponding {@link TransactionFuture} of the {@code reqId} is aborted.
     *
     * @param reqId the {@code ReqId} of the transaction that was aborted
     */
    public void abort(ReqId reqId) {
        synchronized (this) {
            if (state != State.CLOSED) {
                TransactionFuture future = registered.get(reqId);
                if (future != null) {
                    // This is own transaction
                    if (future.complete(false)) {
                        numRegistered--;
                    }
                    notifyAll();
                }
            }
        }
    }

    /**
     * If the transaction monitor is not closed,
     * the corresponding {@link TransactionFuture} of the {@code reqId} is completed with {@code false}.
     *
     * @param reqId the {@code ReqId} of the transaction that was aborted on a Waltz server.
     */
    public void flush(ReqId reqId) {
        synchronized (this) {
            if (state != State.CLOSED) {
                if (registered.containsKey(reqId)) {
                    complete(reqId, false);
                    notifyAll();
                }
            }
        }
    }

    /**
     * @return {@link TransactionFuture} of the transaction that was last enqueued.
     */
    public TransactionFuture lastEnqueued() {
        synchronized (this) {
            return lastReqId != null ? registered.get(lastReqId) : null;
        }
    }

    /**
     * @return the timestamp at which the last transaction was enqueued.
     */
    public long lastEnqueuedTime() {
        synchronized (this) {
            return lastReqId != null ? lastTimestamp : -1L;
        }
    }

    /**
     * @return the total number of transactions currently registered.
     */
    public int registeredCount() {
        synchronized (this) {
            return numRegistered;
        }
    }

    private void complete(ReqId reqId, boolean success) {
        // Iterate over transactions in the insertion order to find expired transactions
        while (!registered.isEmpty()) {
            Iterator<Map.Entry<ReqId, TransactionFuture>> iterator = registered.entrySet().iterator();
            Map.Entry<ReqId, TransactionFuture> entry = iterator.next();
            ReqId other = entry.getKey();
            TransactionFuture future = entry.getValue();

            iterator.remove(); // This is where we actually remove futures from the map.
            future.flushed();

            if (reqId.eq(other)) {
                // Complete the transaction, then stop
                if (future.complete(success)) {
                    numRegistered--;
                }
                break;
            } else {
                // Complete older transactions as failure
                // The strong-ordering guarantees that all pending transactions before this were failed.
                if (future.complete(false)) {
                    numRegistered--;
                }
            }
        }
    }

}
