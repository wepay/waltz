package com.wepay.waltz.test.smoketest;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.zktools.util.Uninterruptibly;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class SmokeTestClientThread extends Thread {

    private static final Logger logger = Logging.getLogger(SmokeTestClientThread.class);

    private final SmokeTestClientCallbacks callbacks;
    private final WaltzClient client;
    private final int numTransactions;
    private final int numLocks;
    private final long sleepBetweenTransactions;
    private final AtomicLong totalResponseTimeNanos = new AtomicLong(0);
    private final AtomicInteger producerChecksum;
    private final AtomicInteger numProduced = new AtomicInteger(0);
    private final Set<UUID> success = new HashSet<>();
    private final ConcurrentMap<UUID, Long> applied = new ConcurrentHashMap<>();
    private final Random rand = new Random();

    SmokeTestClientThread(
        WaltzClientConfig clientConfig,
        int numPartitions,
        int numTransactions,
        int numLocks,
        long sleepBetweenTransactions,
        AtomicInteger producerChecksum
    ) throws Exception {
        this.numTransactions = numTransactions;
        this.numLocks = numLocks;
        this.sleepBetweenTransactions = sleepBetweenTransactions;
        this.producerChecksum = producerChecksum;
        this.callbacks = new SmokeTestClientCallbacks(numPartitions, applied);
        this.client = new WaltzClient(this.callbacks, clientConfig);

        String clientName = "[Client" + this.client.clientId() + "]";
        Thread.currentThread().setName(clientName);
    }

    public void run() {
        try {
            for (int i = 0; i < numTransactions; i++) {
                if (i % 1000 == 0) {
                    System.out.print('-');
                }

                int lock = rand.nextInt(numLocks);

                TxnContext context = new TxnContext(client.clientId(), lock, applied);

                producerChecksum.addAndGet(context.uuid.hashCode());

                long timestampNanos = System.nanoTime();
                context.completionFuture.whenComplete((result, exception) -> {
                    long latency = System.nanoTime() - timestampNanos;
                    totalResponseTimeNanos.addAndGet(latency);
                    numProduced.incrementAndGet();

                    if (result) {
                        synchronized (success) {
                            if (!success.add(context.uuid)) {
                                System.out.print('#');
                                logger.error("duplicate transaction committed");
                            }
                        }
                    } else {
                        logger.error("transaction failed: client=" + client.clientId() + " uuid=" + context.uuid);
                    }
                });

                client.submit(context);

                Thread.yield();
                Uninterruptibly.sleep(sleepBetweenTransactions);
            }

            logger.info("waiting for completion: client=" + client.clientId());
            while (numProduced.get() < numTransactions) {
                if (client.hasPendingTransactions()) {
                    client.flushTransactions();
                } else {
                    Uninterruptibly.sleep(100);
                }
            }
        } catch (Throwable ex) {
            logger.error("terminated by ERROR: client=" + client.clientId(), ex);
        }
    }

    void closeWaltzClient() {
        try {
            client.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    int clientId() {
        return client.clientId();
    }

    int numProduced() {
        return numProduced.get();
    }

    int numConsumed() {
        return callbacks.numConsumed();
    }

    void print() {
        System.out.print("\n#\n"
            + "# WRITE client=" + client.clientId()
            + " numProduced=" + numProduced.get()
            + " unique=" + success.size()
            + " numConsumed=" + callbacks.numConsumed()
            + " hwmarks=" + callbacks.getClientHighWaterMarks()
            + " checksum=" + (callbacks.checksum() == producerChecksum.get() ? "OK" : "ERR")
            + "\n#"
        );
    }

    long totalResponseTimeNanos() {
        return totalResponseTimeNanos.get();
    }

}
