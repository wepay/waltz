package com.wepay.waltz.client.internal;

import java.util.concurrent.Future;

public interface RpcClient {

    void close();

    Future<byte[]> getTransactionData(int partitionId, long transactionId);

}
