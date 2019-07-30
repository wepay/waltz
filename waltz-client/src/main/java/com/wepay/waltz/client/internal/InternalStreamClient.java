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

public class InternalStreamClient extends InternalBaseClient implements StreamClient {

    private static final Logger logger = Logging.getLogger(InternalStreamClient.class);

    private final InternalRpcClient rpcClient;

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

    @Override
    public void onMountingPartition(WaltzNetworkClient networkClient, Partition partition) {
        logger.info("sending MountRequest: {} to={}", partition, networkClient.endpoint);
        networkClient.sendMessage(new MountRequest(partition.nextReqId(), partition.clientHighWaterMark(), networkClient.seqNum));
    }

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

    @Override
    public TransactionBuilderImpl getTransactionBuilder(TransactionContext context) {
        Partition partition = getPartition(context.partitionId(numPartitions));
        partition.ensureMounted();

        return new TransactionBuilderImpl(partition.nextReqId(), callbacks.getClientHighWaterMark(partition.partitionId));
    }

    @Override
    public TransactionFuture append(AppendRequest request) {
        Partition partition = getPartition(request.reqId.partitionId());

        while (true) {
            TransactionFuture future = partition.append(request);

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
