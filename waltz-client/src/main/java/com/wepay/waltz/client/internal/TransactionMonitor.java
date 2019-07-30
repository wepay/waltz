package com.wepay.waltz.client.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.exception.ClientClosedException;
import com.wepay.waltz.exception.PartitionInactiveException;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

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

    public TransactionMonitor(int maxConcurrentTransactions) {
        this.maxConcurrentTransactions = maxConcurrentTransactions;
    }

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

    public boolean isStopped() {
        synchronized (this) {
            return state == State.STOPPED;
        }
    }

    public void clear() {
        synchronized (this) {
            // When clear() is called, it is certain that all append requests from this client are flushed, and
            // there shouldn't be any pending transactions. Complete all pending futures by marking them failed.
            registered.values().forEach(future -> {
                future.complete(false);
                future.flushed();
            });
            registered.clear();
            numRegistered = 0;
            notifyAll();
        }
    }

    public int maxCapacity() {
        return maxConcurrentTransactions;
    }

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

    public TransactionFuture lastEnqueued() {
        synchronized (this) {
            return lastReqId != null ? registered.get(lastReqId) : null;
        }
    }

    public long lastEnqueuedTime() {
        synchronized (this) {
            return lastReqId != null ? lastTimestamp : -1L;
        }
    }

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
