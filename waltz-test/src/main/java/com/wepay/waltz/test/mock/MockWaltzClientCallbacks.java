package com.wepay.waltz.test.mock;

import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MockWaltzClientCallbacks implements WaltzClientCallbacks {

    public final HashMap<Integer, State<Long>> highWaterMarks = new HashMap<>();
    public final HashSet<ReqId> reqIds = new HashSet<>();
    public final LinkedBlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();
    public final ArrayList<Transaction> transactions = new ArrayList<>();

    @Override
    public long getClientHighWaterMark(int partitionId) {
        State<Long> hw = highWaterMarks.get(partitionId);
        if (hw == null) {
            throw new IllegalArgumentException("partition not found");
        }

        return hw.get();
    }

    @Override
    public void applyTransaction(Transaction transaction) {
        int partitionId = transaction.reqId.partitionId();
        long transactionId = transaction.transactionId;
        long expectedTransactionId = getClientHighWaterMark(partitionId) + 1;

        if (expectedTransactionId != transactionId) {
            throw new IllegalStateException(
                "unexpected transaction: expected=" + expectedTransactionId + " actual=" + transactionId
            );
        }

        synchronized (reqIds) {
            if (reqIds.contains(transaction.reqId)) {
                throw new IllegalStateException("duplicate reqId");
            }

            // If processing failed (ex. due to network failure during getTransactionData()), we don't record the transaction and reqId.
            process(transaction);

            synchronized (transactions) {
                transactions.add(transaction);
            }
            reqIds.add(transaction.reqId);
            setClientHighWaterMark(partitionId, transactionId);
            reqIds.notifyAll();
        }
    }

    protected void process(Transaction transaction) {
    }

    @Override
    public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
        exception.printStackTrace();
        exceptions.offer(exception);
    }

    public MockWaltzClientCallbacks setClientHighWaterMark(int partitionId, long highWaterMark) {
        synchronized (highWaterMarks) {
            State<Long> hw = highWaterMarks.get(partitionId);
            if (hw != null) {
                hw.set(highWaterMark);
            } else {
                highWaterMarks.put(partitionId, new State<>(highWaterMark));
            }
        }
        return this;
    }

    public void awaitReqId(ReqId reqId, long timeout) {
        long due = System.currentTimeMillis() + timeout;

        synchronized (reqIds) {
            while (!reqIds.contains(reqId)) {
                try {
                    long remaining = due - System.currentTimeMillis();

                    if (remaining > 0) {
                        reqIds.wait(timeout);
                    } else {
                        return;
                    }
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        }
    }

    public void awaitHighWaterMark(int partitionId, long transactionId, long timeout) {
        State<Long> highWaterMark = highWaterMarks.get(partitionId);

        if (highWaterMark == null) {
            throw new RuntimeException("high-water mark not found: partitionId=" + partitionId);
        }

        final long due = System.currentTimeMillis() + timeout;

        StateChangeFuture<Long> watch = highWaterMark.watch();
        while (watch.currentState < transactionId) {
            long now = System.currentTimeMillis();
            if (due > now) {
                try {
                    if (watch.get(due - now, TimeUnit.MILLISECONDS) >= transactionId) {
                        break;
                    }
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                } catch (Exception ex) {
                    return;
                }
            } else {
                return;
            }
            watch = highWaterMark.watch();
        }
    }

}
