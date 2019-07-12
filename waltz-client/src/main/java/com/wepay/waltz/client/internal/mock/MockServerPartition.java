package com.wepay.waltz.client.internal.mock;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.FeedData;
import com.wepay.waltz.common.message.FlushResponse;
import com.wepay.waltz.common.message.LockFailure;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.DaemonThreadFactory;
import com.wepay.zktools.util.Uninterruptibly;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MockServerPartition {

    private static final Logger logger = Logging.getLogger(MockServerPartition.class);

    public final int partitionId;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(DaemonThreadFactory.INSTANCE);
    private final ArrayList<AbstractMessage> messageList = new ArrayList<>();
    private final ConcurrentHashMap<Integer, byte[]> transactionDataMap = new ConcurrentHashMap<>();
    private final HashMap<Integer, Long> locks = new HashMap<>();

    private final Random random = new Random();

    private long highWaterMark = -1;

    private volatile long maxDelayMillis = 0;
    private volatile double faultRate = 0d;

    MockServerPartition(int partitionId) {
        this.partitionId = partitionId;
    }

    void close() {
        executor.shutdownNow();
    }

    void append(AppendRequest request, boolean forceFailure) {
        executor.submit(new Append(request, forceFailure));
    }

    MessageReader getMessageReader() {
        return new MessageReader(messageList);
    }

    byte[] getTransactionData(long transactionId) {
        synchronized (this) {
            return transactionDataMap.get((int) transactionId).clone();
        }
    }

    void flushTransactions(final ReqId reqId) {
        executor.submit(new Flush(reqId));
    }

    void setMaxDelay(long maxDelayMillis) {
        this.maxDelayMillis = maxDelayMillis;
    }

    void setFaultRate(double faultRate) {
        this.faultRate = faultRate;
    }

    private class Append implements Runnable {

        final AppendRequest request;
        final boolean forceFailure;

        Append(AppendRequest request, boolean forceFailure) {
            this.request = request;
            this.forceFailure = forceFailure;
        }

        public void run() {
            try {
                delay();

                boolean success = !forceFailure && (random.nextDouble() >= faultRate);

                if (success) {
                    long hw = Math.max(
                        getLockHighWaterMark(request.writeLockRequest),
                        getLockHighWaterMark(request.readLockRequest)
                    );

                    if (hw > request.clientHighWaterMark) {
                        sendMessage(new LockFailure(request.reqId, hw));
                        return;
                    }
                }

                if (success) {
                    synchronized (this) {
                        ++highWaterMark;

                        transactionDataMap.put((int) highWaterMark, request.data.clone());
                        sendMessage(new FeedData(copy(request.reqId), highWaterMark, request.header));

                        commit(request.writeLockRequest, highWaterMark);
                    }
                }

            } catch (Exception ex) {
                logger.error("append failed", ex);
            }
        }

    }

    private class Flush implements Runnable {

        private ReqId reqId;

        Flush(ReqId reqId) {
            this.reqId = reqId;
        }

        public void run() {
            synchronized (this) {
                sendMessage(new FlushResponse(reqId, highWaterMark));
            }
        }

    }

    private void sendMessage(AbstractMessage message) {
        synchronized (messageList) {
            messageList.add(message);
            messageList.notifyAll();
        }
    }

    private void delay() {
        int maxDelay = (int) maxDelayMillis;
        if (maxDelay > 0) {
            Uninterruptibly.sleep((long) random.nextInt(maxDelay));
        }
    }

    private long getLockHighWaterMark(int[] lockRequest) {
        long maxHighWaterMark = -1L;
        for (int hash : lockRequest) {
            Long hw = locks.get(hash);
            if (hw != null && hw > maxHighWaterMark) {
                maxHighWaterMark = hw;
            }
        }
        return maxHighWaterMark;
    }

    private void commit(int[] lockRequest, Long highWaterMark) {
        for (int hash : lockRequest) {
            locks.put(hash, highWaterMark);
        }
    }

    private static ReqId copy(ReqId reqId) {
        return new ReqId(reqId.mostSigBits, reqId.leastSigBits);
    }

    public static Map<Integer, MockServerPartition> create(int numPartitions) {
        Map<Integer, MockServerPartition> serverPartitionMap = new HashMap<>();

        for (int i = 0; i < numPartitions; i++) {
            serverPartitionMap.put(i, new MockServerPartition(i));
        }

        return serverPartitionMap;
    }
}
