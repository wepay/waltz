package com.wepay.waltz.client.internal;

import com.wepay.riff.util.Logging;
import com.wepay.riff.util.RequestQueue;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.common.util.QueueConsumerTask;
import org.slf4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * A class representing a queue of {@link TransactionContext}s to be retried.
 * Internally uses a {@link RequestQueue} backed by a {@link LinkedBlockingQueue}.
 */
public class TransactionRetryQueue {

    private static final Logger logger = Logging.getLogger(TransactionRetryQueue.class);

    private final RequestQueue<TransactionContext> queue = new RequestQueue<>(new LinkedBlockingQueue<>());
    private final QueueConsumerTask[] tasks;

    /**
     * Class Constructor.
     *
     * @param waltzClient the {@code WaltzClient} to use to submit {@link TransactionContext}s
     * @param numThread the number of threads to process the retry queue.
     */
    public TransactionRetryQueue(final WaltzClient waltzClient, int numThread) {
        this.tasks = new QueueConsumerTask[numThread];

        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new QueueConsumerTask<TransactionContext>("Txn-Retry-" + i, queue) {
                @Override
                protected void process(TransactionContext context) throws Exception {
                    try {
                        waltzClient.submit(context);
                    } catch (Exception ex) {
                        logger.error("failed to execute transaction: transactionContext=" + context.toString(), ex);
                    }
                }

                @Override
                protected void exceptionCaught(Throwable ex) {
                    // Ignore
                }
            };
        }

        try {
            for (QueueConsumerTask task : tasks) {
                task.start();
            }
        } catch (Throwable ex) {
            close();
            throw ex;
        }
    }

    /**
     * Closes this instance by stopping all retry tasks.
     */
    public void close() {
        for (QueueConsumerTask task : tasks) {
            try {
                task.stop();
            } catch (Throwable ex) {
                // Ignore
            }
        }
    }

    /**
     * Enqueues a {@code context} to be retried.
     * @param context the {@code TransactionContext} to be retried.
     */
    public void enqueue(final TransactionContext context) {
        queue.enqueue(context);
        if (logger.isDebugEnabled()) {
            logger.debug("retry queue size: " + queue.size());
        }
    }

    /**
     * @return the size of the retry queue.
     */
    public int size() {
        return queue.size();
    }

}
