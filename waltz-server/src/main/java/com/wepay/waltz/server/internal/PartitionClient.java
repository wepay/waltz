package com.wepay.waltz.server.internal;

import com.wepay.riff.network.Message;

/**
 * The interface for a client that is interested in writing/reading to/from the partition.
 */
public interface PartitionClient {

    boolean sendMessage(Message msg, boolean flush);

    boolean isActive();

    boolean isWritable();

    Integer clientId();

    Long seqNum();

}
