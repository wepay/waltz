package com.wepay.waltz.test.smoketest;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.PartitionLocalLock;
import com.wepay.waltz.client.TransactionBuilder;
import com.wepay.waltz.client.TransactionContext;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

class TxnContext extends TransactionContext {

    private static final Logger logger = Logging.getLogger(TxnContext.class);

    private static final int REMOVE_SIGN_BIT = 0x7FFFFFFF;


    public final UUID uuid;
    public final CompletableFuture<Boolean> completionFuture = new CompletableFuture<>();

    private final int lock;
    private final ConcurrentMap<UUID, Long> applied;
    private final int clientId;

    private int execCount = 0;

    TxnContext(int clientId, int lock, ConcurrentMap<UUID, Long> applied) {
        this.clientId = clientId;
        this.uuid = UUID.randomUUID();
        this.lock = lock;
        this.applied = applied;
    }

    @Override
    public int partitionId(int numPartitions) {
        return (uuid.hashCode() & REMOVE_SIGN_BIT) % numPartitions;
    }

    @Override
    public boolean execute(TransactionBuilder builder) {
        if (execCount++ > 0) {
            // retry
            System.out.print("+");

            if (applied.containsKey(uuid)) {
                System.out.print('!');
                logger.error("duplicate transaction executed: client=" + clientId);
            }
        }

        builder.setHeader(0);
        builder.setTransactionData(uuid, TxnSerializer.INSTANCE);
        builder.setWriteLocks(Collections.singletonList(new PartitionLocalLock("test", lock)));

        return true;
    }

    @Override
    public void onCompletion(boolean result) {
        completionFuture.complete(result);
    }

    @Override
    public void onException(Throwable ex) {
        logger.error("exception: ", ex);
    }

    @Override
    public String toString() {
        return "[" + clientId + ":" + uuid + "]";
    }

}
