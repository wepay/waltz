package com.wepay.waltz.client;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.BackoffTimer;
import com.wepay.waltz.exception.PartitionNotFoundException;
import com.wepay.waltz.exception.RpcException;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;

/**
 * A class that represents a committed transaction
 */
public class Transaction {

    private static final Logger logger = Logging.getLogger(Transaction.class);

    private static final long INITIAL_RETRY_INTERVAL = 10;
    private static final long MAX_RETRY_INTERVAL = 5000;

    public final ReqId reqId;
    public final long transactionId;

    private final RpcClient rpcClient;
    private final int header;

    public Transaction(long transactionId, int header, ReqId reqId, RpcClient rpcClient) {
        this.reqId = reqId;
        this.transactionId = transactionId;
        this.header = header;
        this.rpcClient = rpcClient;
    }

    /**
     * Returns the transaction header
     * @return the transaction header
     */
    public int getHeader() {
        return header;
    }

    /**
     * Returns the transaction data. This call retrieves the transaction data from a Waltz server over the network.
     * @param serializer the serializer for decoding the transaction data. {@link WaltzClientRuntimeException} will
     *                   be thrown when Waltz client failed to fetch the transaction data. This exception should not
     *                   be caught by an application code in general.
     * @param <T>
     * @return the transaction data
     * @throws WaltzClientRuntimeException This exception should not be caught by an application code in general.
     */
    public <T> T getTransactionData(Serializer<T> serializer) {
        return getTransactionData(serializer, INITIAL_RETRY_INTERVAL, MAX_RETRY_INTERVAL);
    }

    public <T> T getTransactionData(Serializer<T> serializer, final long initialRetryInterval, final long maxRetryInterval) {
        long retryInterval = initialRetryInterval;
        BackoffTimer backoffTimer = null;
        byte[] data = null;

        while (data == null) {
            try {
                data = rpcClient.getTransactionData(reqId.partitionId(), transactionId).get();

            } catch (ExecutionException ex) {
                // Retry if RpcException
                if (ex.getCause() instanceof RpcException) {
                    if (backoffTimer == null) {
                        backoffTimer = new BackoffTimer(maxRetryInterval);
                    }
                    logger.warn("failed to get transaction data, retrying...", ex.getCause());
                    retryInterval = backoffTimer.backoff(retryInterval);
                } else {
                    throw new WaltzClientRuntimeException("failed to get transaction data", ex.getCause());
                }
            } catch (InterruptedException ex) {
                throw new WaltzClientRuntimeException("interrupted", ex);

            } catch (PartitionNotFoundException ex) {
                logger.warn("failed to get transaction data, retrying...", ex);
                if (backoffTimer == null) {
                    backoffTimer = new BackoffTimer(maxRetryInterval);
                }
                retryInterval = backoffTimer.backoff(retryInterval);
            }
        }

        return serializer.deserialize(data);
    }

}
