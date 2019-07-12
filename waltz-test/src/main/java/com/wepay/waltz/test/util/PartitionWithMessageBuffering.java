package com.wepay.waltz.test.util;

import com.wepay.riff.network.Message;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.server.internal.FeedCachePartition;
import com.wepay.waltz.server.internal.Partition;
import com.wepay.waltz.server.internal.PartitionClient;
import com.wepay.waltz.server.internal.PartitionClosedException;
import com.wepay.waltz.server.internal.TransactionFetcher;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.zktools.util.Uninterruptibly;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PartitionWithMessageBuffering extends Partition {

    private LinkedBlockingQueue<Message> msgLog = new LinkedBlockingQueue<>();

    public PartitionWithMessageBuffering(int partitionId, StorePartition storePartition, FeedCachePartition feedCachePartition, TransactionFetcher fetcher, WaltzServerConfig config) {
        super(partitionId, storePartition, feedCachePartition, fetcher, config);
    }

    @Override
    public void receiveMessage(Message msg, PartitionClient sender) throws PartitionClosedException, StoreException {
        msgLog.offer(msg);
        super.receiveMessage(msg, sender);
    }

    public Message nextMessage(long timeout) {
        return Uninterruptibly.call(timeRemaining -> msgLog.poll(timeRemaining, TimeUnit.MILLISECONDS), timeout);
    }

}
