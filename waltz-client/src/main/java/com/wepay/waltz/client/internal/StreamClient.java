package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.common.message.AppendRequest;

import java.util.Set;

/**
 * The interface for implementations of Stream clients to communicate with Waltz cluster.
 */
public interface StreamClient {

    void close();

    int clientId();

    String clusterName();

    TransactionBuilderImpl getTransactionBuilder(TransactionContext context);

    TransactionFuture append(AppendRequest request);

    void flushTransactions();

    void nudgeWaitingTransactions(long longWaitThreshold);

    boolean hasPendingTransactions();

    void setActivePartitions(Set<Integer> partitionIds);

    Set<Integer> getActivePartitions();

}
