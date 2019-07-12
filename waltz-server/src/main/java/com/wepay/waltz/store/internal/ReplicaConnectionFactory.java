package com.wepay.waltz.store.internal;

import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.exception.ReplicaConnectionFactoryClosedException;

public interface ReplicaConnectionFactory {

    ReplicaConnection get(int partitionId, long sessionId) throws StorageRpcException, ReplicaConnectionFactoryClosedException;

    void close();

}
