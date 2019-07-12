package com.wepay.waltz.storage.server.internal;

import com.wepay.riff.network.Message;

public interface PartitionClient {

    boolean sendMessage(Message msg, boolean flush);

}
