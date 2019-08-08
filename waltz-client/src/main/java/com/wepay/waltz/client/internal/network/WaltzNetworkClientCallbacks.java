package com.wepay.waltz.client.internal.network;

import com.wepay.waltz.client.internal.Partition;
import com.wepay.waltz.common.message.ReqId;

/**
 * The interface for WaltzNetworkClient callback methods.
 */
public interface WaltzNetworkClientCallbacks {

    /**
     * Invoked when a partition is being mounted.
     *
     * @param networkClient the {@code WaltzNetworkClient} being used to mount the partition.
     * @param partition the {@code Partition} being mounted
     */
    void onMountingPartition(WaltzNetworkClient networkClient, Partition partition);

    /**
     * Invoked when a {@code WaltzNetworkClient} is disconnected/closed.
     *
     * @param networkClient the {@code WaltzNetworkClient} that is disconnected/closed.
     */
    void onNetworkClientDisconnected(WaltzNetworkClient networkClient);

    /**
     * Invoked when a transaction committed response is received from a waltz server.
     *
     * @param transactionId the id of the received transaction.
     * @param header the header of the received transaction.
     * @param reqId the reqId of the received transaction.
     */
    void onTransactionReceived(long transactionId, int header, ReqId reqId);

}
