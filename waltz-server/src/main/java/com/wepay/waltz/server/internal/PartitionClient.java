package com.wepay.waltz.server.internal;

import com.wepay.riff.network.Message;

public interface PartitionClient {

    boolean sendMessage(Message msg, boolean flush);

    boolean isActive();

    boolean isWritable();

    Integer clientId();

    Long seqNum();

}
