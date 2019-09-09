package com.wepay.waltz.test.smoketest;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.zktools.util.State;
import org.slf4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

class SmokeTestClientCallbacks implements WaltzClientCallbacks {

    private static final Logger logger = Logging.getLogger(SmokeTestClientCallbacks.class);

    private final AtomicInteger numConsumed = new AtomicInteger(0);
    private final AtomicInteger checksum = new AtomicInteger(0);
    private final ConcurrentMap<UUID, Long> applied;
    private final ConcurrentMap<Integer, HighWaterMark> highWaterMarks = new ConcurrentHashMap<>();

    SmokeTestClientCallbacks(int numPartitions, ConcurrentMap<UUID, Long> applied) {
        this.applied = applied;

        for (int i = 0; i < numPartitions; i++) {
            // Use -1 to start from the beginning.
            highWaterMarks.putIfAbsent(i, new HighWaterMark(-1L));
        }
    }

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

        State<Long> hw = highWaterMarks.get(partitionId);

        synchronized (hw) {
            long expectedTransactionId = hw.get() + 1;

            if (expectedTransactionId != transactionId) {
                throw new IllegalStateException(
                    "unexpected transaction: expected=" + expectedTransactionId + " actual=" + transactionId
                );
            } else {
                UUID uuid = transaction.getTransactionData(TxnSerializer.INSTANCE);

                checksum.addAndGet(uuid.hashCode());

                if (applied != null) {
                    Long appliedTxn = applied.putIfAbsent(uuid, transactionId);
                    if (appliedTxn != null) {
                        System.out.print('!');
                        logger.error("duplicate transaction applied: this=" + transactionId + " prev=" + appliedTxn
                            + " reqId=" + transaction.reqId + " uuid=" + uuid);
                    }
                }

                hw.set(transactionId);
            }
        }

        if (numConsumed.incrementAndGet() % 1000 == 0) {
            System.out.print('.');
        }
    }

    @Override
    public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
        logger.error("exception caught: partitionId=" + partitionId + " transactionId=" + transactionId, exception);
    }

    public Map<Integer, Long> getClientHighWaterMarks() {
        TreeMap<Integer, Long> map = new TreeMap<>();

        for (
            Map.Entry<Integer, HighWaterMark> entry : highWaterMarks.entrySet()) {
            map.put(entry.getKey(), entry.getValue().get());
        }

        return map;
    }

    public int numConsumed() {
        return numConsumed.get();
    }

    public int checksum() {
        return checksum.get();
    }

}
