package com.wepay.waltz.server.internal;

import com.wepay.riff.network.Message;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.FeedRequest;
import com.wepay.waltz.common.message.MessageType;
import com.wepay.waltz.common.message.MountRequest;
import com.wepay.waltz.common.message.MountResponse;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.test.mock.MockStore;
import com.wepay.waltz.test.mock.MockStorePartition;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PartitionTest {

    private static final long TIMEOUT = 100000L;
    private static final int PARTITION_ID = 100;
    private static final int NUM_TXN = 3;
    private static final AtomicLong SEQ = new AtomicLong(0);
    private static final int[] NO_LOCK = new int[0];
    private static final int HEADER = 0;
    private static final int TRANSACTION_CACHE_SIZE = 33554432; // 32MB
    private static final int FEED_CACHE_SIZE = 33554432; // 32MB

    private final AtomicLong seqNumGenerator = new AtomicLong(0);
    private final Random rand = new Random();
    private final AtomicInteger seqNum = new AtomicInteger(rand.nextInt());
    private final WaltzServerConfig config = new WaltzServerConfig(new Properties());

    private MockStore store = null;
    private MockStorePartition storePartition = null;
    private FeedCache feedCache = null;
    private FeedCachePartition feedCachePartition = null;
    private TransactionFetcher fetcher = null;

    @Before
    public void setup() {
        store = new MockStore();
        storePartition = (MockStorePartition) store.getPartition(PARTITION_ID, 0);
        feedCache = new FeedCache(FEED_CACHE_SIZE, null);
        feedCachePartition = feedCache.getPartition(PARTITION_ID);
        fetcher = new TransactionFetcher(TRANSACTION_CACHE_SIZE, false, null);
    }

    @After
    public void teardown() {
        if (storePartition != null) {
            storePartition = null;
        }
        if (store != null) {
            store.close();
            store = null;
        }
        if (fetcher != null) {
            fetcher.close();
            fetcher = null;
        }
        if (feedCache != null) {
            feedCache.close();
            feedCache = null;
        }
    }

    @Test
    public void testMount() throws Exception {
        Partition partition = new Partition(PARTITION_ID, storePartition, feedCachePartition, fetcher, config);
        partition.open();
        try {
            int clientId1 = 0;
            int clientId2 = 1;
            int clientId3 = 2;

            final long clientHighWaterMark = -1L;
            Message msg;

            MockPartitionClient partitionClient1 = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId1);
            partition.setPartitionClient(partitionClient1);
            partition.receiveMessage(
                new MountRequest(reqId(clientId1), clientHighWaterMark, partitionClient1.seqNum()),
                partitionClient1
            );

            msg = partitionClient1.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());
            assertTrue(((MountResponse) msg).partitionReady);

            // Append
            append(partition, partitionClient1, NUM_TXN);

            Uninterruptibly.run(() -> storePartition.await(NUM_TXN - 1, TIMEOUT));

            // Connect the second client
            MockPartitionClient partitionClient2 = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId2);
            partition.setPartitionClient(partitionClient2);
            partition.receiveMessage(
                new MountRequest(reqId(clientId2), clientHighWaterMark, partitionClient2.seqNum),
                partitionClient2
            );

            assertEquals(NUM_TXN, readFeedData(partitionClient2, NUM_TXN, TIMEOUT));

            msg = partitionClient2.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());
            assertTrue(((MountResponse) msg).partitionReady);

            // The first client should not see any feed yet because the feed request if not sent yet
            msg = partitionClient1.nextMessage(1);
            assertNull(msg);

            // Send the feed request for the first client
            partition.receiveMessage(
                new FeedRequest(reqId(clientId1), clientHighWaterMark),
                partitionClient1
            );
            // Now the first client should receive feeds
            assertEquals(NUM_TXN, readFeedData(partitionClient1, NUM_TXN, TIMEOUT));

            // Feed won't suspend
            msg = partitionClient1.nextMessage(1);
            assertNull(msg);

            // Connect the third client
            MockPartitionClient partitionClient3 = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId3);
            partition.setPartitionClient(partitionClient3);
            partition.receiveMessage(
                new MountRequest(reqId(clientId1), clientHighWaterMark + 1, partitionClient3.seqNum()),
                partitionClient3
            );
            assertEquals(NUM_TXN - 1, readFeedData(partitionClient3, NUM_TXN - 1, TIMEOUT));

            msg = partitionClient3.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());
            assertTrue(((MountResponse) msg).partitionReady);

        } finally {
            partition.close();
        }
    }

    @Test
    public void testFeedMinFetchSize() throws Exception {
        Partition partition = new Partition(PARTITION_ID, storePartition, feedCachePartition, fetcher, config);
        partition.open();
        try {
            int clientId = 0;
            long clientHighWaterMark = -1L;
            Message msg;

            MockPartitionClient partitionClient = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId);
            partition.setPartitionClient(partitionClient);
            partition.receiveMessage(
                new MountRequest(reqId(clientId), clientHighWaterMark, partitionClient.seqNum()),
                partitionClient
            );

            msg = partitionClient.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());

            // Send feed request before data are stored.
            partition.receiveMessage(
                new FeedRequest(reqId(clientId), clientHighWaterMark),
                partitionClient
            );

            // Append
            append(partition, partitionClient, (int) partition.minFetchSize * 2);

            Uninterruptibly.run(() -> storePartition.await(partition.minFetchSize * 2 - 1, TIMEOUT));

            // Get feed data (the first batch)
            assertEquals(partition.minFetchSize, readFeedData(partitionClient, (int) partition.minFetchSize, TIMEOUT));
            clientHighWaterMark += partition.minFetchSize;

            // Feed suspended
            msg = partitionClient.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.FEED_SUSPENDED, msg.type());

            // Send feed request again
            partition.receiveMessage(
                new FeedRequest(reqId(clientId), clientHighWaterMark),
                partitionClient
            );

            // Get feed data (the second batch)
            assertEquals(partition.minFetchSize, readFeedData(partitionClient, (int) partition.minFetchSize, TIMEOUT));

            // Feed suspended
            msg = partitionClient.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.FEED_SUSPENDED, msg.type());

        } finally {
            partition.close();
        }
    }

    @Test
    public void testFeed() throws Exception {
        Partition partition = new Partition(PARTITION_ID, storePartition, feedCachePartition, fetcher, config);
        partition.open();
        try {
            int clientId = 0;
            long clientHighWaterMark = -1L;
            Message msg;

            MockPartitionClient partitionClient = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId);
            partition.setPartitionClient(partitionClient);
            partition.receiveMessage(
                new MountRequest(reqId(clientId), clientHighWaterMark, partitionClient.seqNum()),
                partitionClient
            );

            msg = partitionClient.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());

            // Append
            append(partition, partitionClient, (int) partition.minFetchSize * 2);

            Uninterruptibly.run(() -> storePartition.await(partition.minFetchSize * 2 - 1, TIMEOUT));

            // Send feed request after data are stored.
            partition.receiveMessage(
                new FeedRequest(reqId(clientId), clientHighWaterMark),
                partitionClient
            );

            // Get the feed data (all data in one batch, thus no intervening suspension)
            for (int i = 0; i < partition.minFetchSize * 2; i++) {
                msg = partitionClient.nextMessage(TIMEOUT);
                assertNotNull(msg);
                assertEquals(MessageType.FEED_DATA, msg.type());
            }

            // Feed suspended
            msg = partitionClient.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.FEED_SUSPENDED, msg.type());

        } finally {
            partition.close();
        }
    }

    @Test
    public void testFeedContextCleanUp() throws Exception {
        Partition partition = new Partition(PARTITION_ID, storePartition, feedCachePartition, fetcher, config);
        partition.open();
        try {
            int clientId1 = 0;
            int clientId2 = 1;
            long clientHighWaterMark = -1L;
            Message msg;

            MockPartitionClient partitionClient1 = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId1);
            partition.setPartitionClient(partitionClient1);
            partition.receiveMessage(
                new MountRequest(reqId(clientId1), clientHighWaterMark, partitionClient1.seqNum()),
                partitionClient1
            );

            msg = partitionClient1.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());

            // Append
            append(partition, partitionClient1, 10);

            // Send feed request
            partition.receiveMessage(
                new FeedRequest(reqId(clientId1), clientHighWaterMark),
                partitionClient1
            );

            // Get feed data (all data in one batch, thus no intervening suspension)
            for (int i = 0; i < 10; i++) {
                msg = partitionClient1.nextMessage(TIMEOUT);
                assertNotNull(msg);
                assertEquals(MessageType.FEED_DATA, msg.type());
                clientHighWaterMark = ((FeedData) msg).transactionId;
            }

            // Feed won't suspend
            msg = partitionClient1.nextMessage(1);
            assertNull(msg);

            // There should be one context
            assertEquals(1, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(0, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(0, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

            // Now invalidate the partitionClient1 (a scenario for a client stop, a network failure, etc.)
            partitionClient1.setActive(false);
            partitionClient1.numSentMessages(0);

            // Append
            append(partition, partitionClient1, 10);

            final long transactionId = clientHighWaterMark + 1;
            Uninterruptibly.run(() -> storePartition.await(transactionId, TIMEOUT));

            // Connect the second client to make sure that the first client is cleaned up.
            MockPartitionClient partitionClient2 = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId2);
            partition.setPartitionClient(partitionClient2);

            // clientHighWaterMark of -1 to force the partitionClient2 to start from the beginning of the feed.
            partition.receiveMessage(
                new MountRequest(reqId(clientId2), -1, partitionClient2.seqNum()),
                partitionClient2
            );

            // partitionClient2 needs to read all 20 messages to guarantee that partitionClient1 has been kicked out.
            assertEquals(20, readFeedData(partitionClient2, 20, TIMEOUT));

            // Force everything in the queue to be fully processed before checking counters. Otherwise, we have a race
            // condition where messages could be sent back to partitionClient2, but the totalRealtimeFeedContextRemoved
            // counter hasn't yet been updated. The process() method in the FeedTask sends responses back before
            // changing the totalAdded/totalRemoved counters. Thus, there's a race condition where the FEED_SUSPENDED
            // response could be sent, but the totalRemovedCounter might not be incremented yet.
            partition.close();

            assertEquals(0, partitionClient1.numSentMessages());
            assertTrue(partitionClient1.numNotSentMessages() <= 1);

            assertEquals(2, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(2, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(0, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

        } finally {
            if (!partition.isClosed()) {
                partition.close();
            }
        }
    }

    @Test
    public void testPauseResumeFeedContext() throws Exception {
        Properties props = new Properties();
        props.setProperty(WaltzServerConfig.MIN_FETCH_SIZE, "3000");
        WaltzServerConfig config = new WaltzServerConfig(props);
        Partition partition = new Partition(PARTITION_ID, storePartition, feedCachePartition, fetcher, config);
        partition.open();
        try {
            int clientId1 = 0;
            int clientId2 = 1;
            Message msg;

            MockPartitionClient partitionClient1 = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId1);
            partition.setPartitionClient(partitionClient1);
            partition.receiveMessage(
                new MountRequest(reqId(clientId1), -1L, partitionClient1.seqNum()),
                partitionClient1
            );

            // Connect the second client to make sure that the first client is paused.
            MockPartitionClient partitionClient2 = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId2);
            partition.setPartitionClient(partitionClient2);
            partition.receiveMessage(
                new MountRequest(reqId(clientId2), -1L, partitionClient2.seqNum()),
                partitionClient2
            );

            msg = partitionClient1.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());
            partitionClient1.numSentMessages(0);

            msg = partitionClient2.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());
            partitionClient2.numSentMessages(0);

            // partitionClient1 sends the feed request
            partition.receiveMessage(
                new FeedRequest(reqId(clientId1), -1L),
                partitionClient1
            );

            // partitionClient1 sends the feed request
            partition.receiveMessage(
                new FeedRequest(reqId(clientId2), -1L),
                partitionClient2
            );

            // Append
            append(partition, partitionClient1, 10);

            // partitionClient1 gets feed data (all data in one batch, thus no intervening suspension)
            assertEquals(10, readFeedData(partitionClient1, 10, TIMEOUT));

            // partitionClient2 gets feed data (all data in one batch, thus no intervening suspension)
            assertEquals(10, readFeedData(partitionClient2, 10, TIMEOUT));

            assertEquals(2, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(0, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(0, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

            // Now make the partitionClient2 non-writable
            partitionClient2.setWritable(false);

            // Append
            append(partition, partitionClient1, 10);

            // partitionClient1 gets feed data (all data in one batch, thus no intervening suspension)
            assertEquals(10, readFeedData(partitionClient1, 10, TIMEOUT));

            // partitionClient2 gets no data (all data in one batch, thus no intervening suspension)
            assertEquals(0, readFeedData(partitionClient2, 1, 100));

            // One feed context is removed
            assertEquals(2, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(1, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(0, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

            // This should be no-op
            partition.resumePausedFeedContexts();

            assertEquals(2, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(1, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(0, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

            // Now make the partitionClient2 writable
            partitionClient2.setWritable(true);

            // This should resume the feed.
            partition.resumePausedFeedContexts();

            assertEquals(3, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(1, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(0, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

            // partitionClient2 gets feed data (all data in one batch, thus no intervening suspension)
            assertEquals(10, readFeedData(partitionClient2, 10, TIMEOUT));

            // Now make the partitionClient2 non-writable again
            partitionClient2.setWritable(false);

            // Append many transactions to leave partitionClient2 far behind
            append(partition, partitionClient1, 2000);

            // Force partitionClient2 to be removed by making sure all messages in the FeedContext queue have
            // been drained.
            assertEquals(2000, readFeedData(partitionClient1, 2000, TIMEOUT));

            // one feed context is removed
            assertEquals(3, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(2, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(0, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

            // partitionClient2 gets no data (all data in one batch, thus no intervening suspension)
            assertEquals(0, readFeedData(partitionClient2, 1, 100));

            // Now make the partitionClient2 writable again
            partitionClient2.setWritable(true);

            // This should resume the feed as catch-up.
            partition.resumePausedFeedContexts();

            assertEquals(3, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(2, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(1, partition.getTotalCatchupFeedContextAdded());
            assertEquals(0, partition.getTotalCatchupFeedContextRemoved());

            // Client1 and Client2 get feed data
            assertEquals(2000, readFeedData(partitionClient2, 2000, TIMEOUT));

            assertEquals(2020,  partitionClient1.numSentMessages());
            assertEquals(2020,  partitionClient2.numSentMessages());

        } finally {
            partition.close();
        }
    }

    @Test
    public void testCatchUp() throws Exception {
        Partition partition = new Partition(PARTITION_ID, storePartition, feedCachePartition, fetcher, config);
        partition.open();
        try {
            int clientId = 0;
            long clientHighWaterMark = -1L;
            Message msg;

            MockPartitionClient partitionClient = new MockPartitionClient(seqNumGenerator.getAndIncrement(), clientId);
            partition.setPartitionClient(partitionClient);
            partition.receiveMessage(
                new MountRequest(reqId(clientId), clientHighWaterMark, partitionClient.seqNum()),
                partitionClient
            );

            msg = partitionClient.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.MOUNT_RESPONSE, msg.type());

            // Append
            append(partition, partitionClient, (int) partition.realtimeThreshold);

            Uninterruptibly.run(() -> storePartition.await(partition.realtimeThreshold - 1, TIMEOUT));

            // Send feed request after data are stored. The fetch size is the size of the data set.
            partition.receiveMessage(
                new FeedRequest(reqId(clientId), clientHighWaterMark),
                partitionClient
            );

            // Get feed data (all data in one batch, thus no intervening suspension)
            for (int i = 0; i < partition.realtimeThreshold; i++) {
                msg = partitionClient.nextMessage(TIMEOUT);
                assertNotNull(msg);
                assertEquals(MessageType.FEED_DATA, msg.type());
            }

            // Feed suspended
            msg = partitionClient.nextMessage(TIMEOUT);
            assertNotNull(msg);
            assertEquals(MessageType.FEED_SUSPENDED, msg.type());

            // Close the partition as a way to force the queue to process all messages fully before checking asserts.
            // The process() method in the FeedTask sends responses back before changing the totalAdded/totalRemoved
            // counters. Thus, there's a race condition where the FEED_SUSPENDED response could be sent, but the
            // totalRemovedCounter might not be incremented yet.
            partition.close();

            assertEquals(0, partition.getTotalRealtimeFeedContextAdded());
            assertEquals(0, partition.getTotalRealtimeFeedContextRemoved());
            assertEquals(1, partition.getTotalCatchupFeedContextAdded());
            assertEquals(1, partition.getTotalCatchupFeedContextRemoved());

        } finally {
            if (!partition.isClosed()) {
                partition.close();
            }
        }
    }

    private ReqId reqId(int clientId) {
        return new ReqId(clientId, 0, PARTITION_ID, seqNum.incrementAndGet());
    }

    private static byte[] data() {
        return Long.toOctalString(SEQ.incrementAndGet()).getBytes(StandardCharsets.UTF_8);
    }

    private void append(
        Partition partition,
        PartitionClient partitionClient,
        int numTransactions
    ) throws Exception {
        for (int i = 0; i < numTransactions; i++) {
            byte[] data = data();
            partition.receiveMessage(
                new AppendRequest(reqId(partitionClient.clientId()), -1L, NO_LOCK, NO_LOCK, HEADER, data, Utils.checksum(data)),
                partitionClient
            );
        }
    }

    private int readFeedData(MockPartitionClient partitionClient, int numTransactions, long timeout) {
        int readCount = 0;
        for (int i = 0; i < numTransactions; i++) {
            Message msg = partitionClient.nextMessage(timeout);
            if (msg == null || msg.type() != MessageType.FEED_DATA) {
                break;
            }
            readCount++;
        }
        return readCount;
    }

    private static class MockPartitionClient implements PartitionClient {

        private final Integer clientId;
        private final Long seqNum;

        private boolean writable = true;
        private boolean active = true;
        private int numSentMessages = 0;
        private int numNotSentMessages = 0;

        private LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();

        MockPartitionClient(long seqNum, int clientId) {
            this.seqNum = seqNum;
            this.clientId = clientId;
        }

        @Override
        public boolean sendMessage(Message msg, boolean flush) {
            synchronized (this) {
                if (active) {
                    messages.offer(msg);
                    numSentMessages++;
                    return true;
                } else {
                    numNotSentMessages++;
                    return false;
                }
            }
        }

        @Override
        public boolean isWritable() {
            synchronized (this) {
                return writable;
            }
        }

        @Override
        public boolean isActive() {
            synchronized (this) {
                return active;
            }
        }

        @Override
        public Long seqNum() {
            return seqNum;
        }

        @Override
        public Integer clientId() {
            return clientId;
        }

        public void setActive(boolean value) {
            synchronized (this) {
                active = value;
            }
        }

        public void setWritable(boolean value) {
            synchronized (this) {
                writable = value;
            }
        }

        public Message nextMessage(long timeout) {
            return Uninterruptibly.call(timeRemaining -> messages.poll(timeRemaining, TimeUnit.MILLISECONDS), timeout);
        }

        public int numSentMessages() {
            synchronized (this) {
                return numSentMessages;
            }
        }

        public void numSentMessages(int n) {
            synchronized (this) {
                numSentMessages = n;
            }
        }

        public int numNotSentMessages() {
            synchronized (this) {
                return numNotSentMessages;
            }
        }

        public void numNotSentMessages(int n) {
            synchronized (this) {
                numNotSentMessages = n;
            }
        }

    }
}
