package com.wepay.waltz.client;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.client.internal.StreamClient;
import com.wepay.waltz.client.internal.TransactionBuilderImpl;
import com.wepay.waltz.client.internal.TransactionResultHandler;
import com.wepay.waltz.client.internal.TransactionRetryQueue;
import com.wepay.waltz.client.internal.WaltzClientDriver;
import com.wepay.waltz.client.internal.WaltzClientDriverImpl;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.util.DaemonThreadFactory;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ManagedClient;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The main class of Waltz Client API.
 */
public class WaltzClient {

    private static final Logger logger = Logging.getLogger(WaltzClient.class);

    private final WaltzClientDriver driver;
    private final ManagedClient managedClient;
    private final ClusterManager clusterManager;
    private final RpcClient rpcClient;
    protected final StreamClient streamClient;

    private final Object lock = new Object();

    private final TransactionRetryQueue transactionRetryQueue;
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * Class Constructor.
     *
     * @param callbacks {@link WaltzClientCallbacks}.
     * @param config {@link WaltzClientConfig}.
     * @throws Exception if an exception was encountered while creating a new WaltzClient instance.
     */
    public WaltzClient(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception {
        this(
            getWaltzClientDriver(callbacks, config),
            (int) config.get(WaltzClientConfig.NUM_TRANSACTION_RETRY_THREADS),
            (long) config.get(WaltzClientConfig.LONG_WAIT_THRESHOLD)
        );
    }

    /**
     * Class Constructor, uses the provided {@link WaltzClientDriver}.
     *
     * @param driver a {@link WaltzClientDriver} instance backing this {@code WaltzClient}.
     * @param numTransactionRetryThreads the number of transaction retry threads.
     * @param longWaitThreshold How long to wait before nudging waiting transactions
     */
    public WaltzClient(WaltzClientDriver driver, int numTransactionRetryThreads, long longWaitThreshold) {
        this.driver = driver;
        this.rpcClient = driver.getRpcClient();
        this.streamClient = driver.getStreamClient();
        this.clusterManager = driver.getClusterManager();
        this.managedClient = driver.getManagedClient();
        this.transactionRetryQueue =
            numTransactionRetryThreads > 0 ? new TransactionRetryQueue(this, numTransactionRetryThreads) : null;

        this.scheduledExecutorService = Executors.newScheduledThreadPool(1, new DaemonThreadFactory());
        this.scheduledExecutorService.scheduleWithFixedDelay(
            () -> this.streamClient.nudgeWaitingTransactions(longWaitThreshold),
            longWaitThreshold,
            longWaitThreshold,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Returns the client id of this instance. A client id is a unique id assigned to an instance of WaltzClient on creation.
     *
     * @return the client id.
     */
    public int clientId() {
        return streamClient.clientId();
    }

    /**
     * Returns the name of the Waltz cluster.
     *
     * @return the cluster name.
     */
    public String clusterName() {
        return streamClient.clusterName();
    }

    /**
     * Closes the client. The client becomes unusable after this call.
     */
    public void close() {
        synchronized (lock) {
            try {
                clusterManager.unmanage(managedClient);
            } catch (Throwable ex) {
                logger.error("failed to unmanage client", ex);
            }
            try {
                scheduledExecutorService.shutdownNow();
            } catch (Throwable ex) {
                logger.error("failed to shutdown scheduledExecutorService", ex);
            }
            try {
                if (transactionRetryQueue != null) {
                    transactionRetryQueue.close();
                }
            } catch (Throwable ex) {
                logger.error("failed to close retry queue", ex);
            }
            try {
                driver.close();
            } catch (Throwable ex) {
                logger.error("failed to close client driver", ex);
            }
        }
    }

    /**
     * Submits a transaction context.
     * Waltz client will immediately call {@link TransactionContext#execute(TransactionBuilder)}.
     * <p>
     * If {@link TransactionContext#execute(TransactionBuilder)} returns true,
     * Waltz client builds the transaction from {@link TransactionBuilder} and
     * send an append request to the Waltz server.
     * If append fails, the client automatically attempts retries until it succeeds or expires as long as the client is active.
     * </p>
     * <p>
     * If {@link TransactionContext#execute(TransactionBuilder)} returns false, the transaction is ignored by Waltz client
     * </p>
     * <p>
     *
     * @param context the transaction context
     */
    public void submit(TransactionContext context) {
        try {
            // Build the transaction.
            AppendRequest request = build(context);

            if (request != null) {
                streamClient.append(request, context)
                    .whenComplete(new TransactionResultHandlerImpl(context));
            } else {
                // Completed with no append. There will be no retry.
                context.onCompletion(false);
            }
        } catch (Throwable ex) {
            // The transaction failed due to an exception. There will be no retry.
            context.onException(ex);
            throw ex;
        }
    }

    protected AppendRequest build(TransactionContext context) {
        TransactionBuilderImpl builder = streamClient.getTransactionBuilder(context);
        AppendRequest request = null;

        // Execute the transaction context with the builder.
        if (context.execute(builder)) {
            // Build the transaction request.
            request = builder.buildRequest();
        }

        return request;
    }

    /**
     * Waits for all current pending append requests to finish with either success or failure.
     */
    public void flushTransactions() {
        streamClient.flushTransactions();
    }

    /**
     * Checks if there is any pending append request.
     * @return true if there are pending append requests, otherwise false.
     */
    public boolean hasPendingTransactions() {
        return streamClient.hasPendingTransactions() || transactionRetryQueue.size() > 0;
    }

    /**
     * Sets ids of partitions for this client to work with.
     * Partitions not in {@code partitionIds} will become inaccessible from this client.
     * To use this method the client must set the {@code client.autoMount} configuration parameter to {@code false}.
     *
     * @param partitionIds the set of partition ids.
     */
    public void setPartitions(Set<Integer> partitionIds) {
        streamClient.setActivePartitions(partitionIds);
    }

    /**
     * Gets ids of partitions this client is working with.
     *
     * @return Set of partition integers.
     */
    public Set<Integer> getPartitions() {
        return streamClient.getActivePartitions();
    }

    /**
     * Gets current high watermark of a partition.
     * @return
     */
    public long getHighWaterMark(int partitionId) throws ExecutionException, InterruptedException {
        return rpcClient.getHighWaterMark(partitionId).get();
    }

    // Executes the transaction asynchronously. This is used in retrying a failed transaction.
    private void executeAsync(TransactionContext context) {
        if (transactionRetryQueue != null) {
            transactionRetryQueue.enqueue(context);
        }
    }

    private static WaltzClientDriver getWaltzClientDriver(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception {
        WaltzClientDriver driver = (WaltzClientDriver) config.getObject(WaltzClientConfig.MOCK_DRIVER);
        if (driver == null) {
            driver = new WaltzClientDriverImpl();
        }
        driver.initialize(callbacks, config);

        return driver;
    }

    private class TransactionResultHandlerImpl extends TransactionResultHandler {

        private final TransactionContext context;

        TransactionResultHandlerImpl(TransactionContext context) {
            this.context = context;
        }

        @Override
        protected void onSuccess() {
            context.onCompletion(true);
        }

        @Override
        protected void onFailure() {
            executeAsync(context);
        }

        @Override
        protected void onException(Throwable exception) {
            context.onException(exception);
        }

    }

}
