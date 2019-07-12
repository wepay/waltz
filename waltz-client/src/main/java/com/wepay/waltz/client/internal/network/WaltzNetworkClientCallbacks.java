package com.wepay.waltz.client.internal.network;

import com.wepay.waltz.client.internal.Partition;
import com.wepay.waltz.common.message.ReqId;

public interface WaltzNetworkClientCallbacks {

    void onMountingPartition(WaltzNetworkClient networkClient, Partition partition);

    void onNetworkClientDisconnected(WaltzNetworkClient networkClient);

    void onTransactionReceived(long transactionId, int header, ReqId reqId);

}
