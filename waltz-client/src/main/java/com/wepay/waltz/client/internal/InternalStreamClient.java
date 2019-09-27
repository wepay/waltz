package com.wepay.waltz.client.internal;

import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.internal.network.WaltzNetworkClient;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.MountRequest;
import com.wepay.waltz.common.message.ReqId;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;

/**
 * An internal implementation of {@link StreamClient}, extending {@link InternalBaseClient}.
 */
public class InternalStreamClient extends InternalBaseClient implements StreamClient {

    private static final Logger logger = Logging.getLogger(InternalStreamClient.class);

    private final InternalRpcClient rpcClient;

    /**
     * Class Constructor.
     *
     * @param autoMount if {@code true}, mounts all partitions.
     * @param sslCtx {@link SslContext}
     * @param maxConcurrentTransactions the maximum number of concurrent transactions
     * @param callbacks {@link WaltzClientCallbacks}
     * @param rpcClient {@link InternalRpcClient}
     * @param threadPool {@link MessageProcessingThreadPool}
     */
    public InternalStreamClient(
        boolean autoMount,
        SslContext sslCtx,
        int maxConcurrentTransactions,
        WaltzClientCallbacks callbacks,
        InternalRpcClient rpcClient,
        MessageProcessingThreadPool threadPool
    ) {
        super(autoMount, sslCtx, maxConcurrentTransactions, callbacks, threadPool);
        this.rpcClient = rpcClient;
    }

    /**
     * Invoked when a {@link Partition} is being mounted.
     *
     * @param networkClient the {@code WaltzNetworkClient} being to mount the partition.
     * @param partition the {@code Partition} being mounted.
     */
    @Override
    public void onMountingPartition(WaltzNetworkClient networkClient, Partition partition) {
        logger.info("sending MountRequest: {} to={}", partition, networkClient.endpoint);
        networkClient.sendMessage(new MountRequest(partition.nextReqId(), partition.clientHighWaterMark(), networkClient.seqNum));
    }

    /**
     * Invoked when a transaction committed response is received from a waltz server.
     * Internally, invokes {@link WaltzClientCallbacks#applyTransaction(Transaction)}.
     *
     * @param transactionId the id of the received transaction.
     * @param header the header of the received transaction.
     * @param reqId the reqId of the received transaction.
     */
    @Override
    public void onTransactionReceived(long transactionId, int header, ReqId reqId) {
        try {
            callbacks.applyTransaction(new Transaction(transactionId, header, reqId, rpcClient));

        } catch (Throwable ex) {
            logger.error("failed to apply transaction: partitionId=" + reqId.partitionId() + " transactionId=" + transactionId, ex);
            try {
                callbacks.uncaughtException(reqId.partitionId(), transactionId, ex);
            } catch (Exception e) {
                logger.error("callback error [uncaughtException]", e);
            }
            throw ex;
        }
    }

    /**
     * Returns a {@link TransactionBuilderImpl} for a given {@link TransactionContext}.
     *
     * @param context the {@code TransactionContext} to get a transaction builder for.
     * @return the corresponding {@code TransactionBuilderImpl} instance.
     */
    @Override
    public TransactionBuilderImpl getTransactionBuilder(TransactionContext context) {
        Partition partition = getPartition(context.partitionId(numPartitions));
        partition.ensureMounted();

        return new TransactionBuilderImpl(partition.nextReqId(), callbacks.getClientHighWaterMark(partition.partitionId));
    }

    /**
     * Appends the data to the given partition.
     * Internally, an append request is sent to the waltz server corresponding to the given partition.
     *
     * @param request the {@link AppendRequest} with actual payload, partition info, etc.
     * @return a {@link TransactionFuture} which completes when the append response is processed.
     */
    @Override
    public TransactionFuture append(AppendRequest request, TransactionContext context) {
        Partition partition = getPartition(request.reqId.partitionId());

        while (true) {
            TransactionFuture future = partition.append(request, context);

            if (future != null) {
                return future;
            } else {
                // Transaction registration timed out. Flush pending transactions and retry.
                future = partition.flushTransactionsAsync();
                if (future != null) {
                    future.awaitFlush();

                } else {
                    logger.debug("null returned from flushTransactionsAsync");
                }
            }
        }
    }

}
