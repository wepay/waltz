package com.wepay.waltz.test.mock;

import com.wepay.waltz.client.PartitionLocalLock;
import com.wepay.waltz.client.TransactionBuilder;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.test.util.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class MockContext extends TransactionContext {

    public final CompletableFuture<Boolean> future = new CompletableFuture<>();
    public final CompletableFuture<Boolean> applicationFuture = new CompletableFuture<>();
    public final CompletableFuture<Boolean> lockFailureFuture = new CompletableFuture<>();
    public final AtomicInteger execCount = new AtomicInteger(0);

    private final int partitionId;
    private final int header;
    private final String data;
    private final List<PartitionLocalLock> writeLocks;
    private final List<PartitionLocalLock> readLocks;
    private final boolean retry;

    public static Builder builder() {
        return new Builder();
    }

    MockContext(int partitionId, int header, String data, List<PartitionLocalLock> writeLocks, List<PartitionLocalLock> readLocks, boolean retry) {
        super();
        this.partitionId = partitionId;
        this.header = header;
        this.data = data;
        this.writeLocks = writeLocks;
        this.readLocks = readLocks;
        this.retry = retry;
    }

    @Override
    public int partitionId(int numPartitions) {
        return partitionId;
    }

    @Override
    public boolean execute(TransactionBuilder builder) {
        // Execute only once for testing purpose.
        if (execCount.get() == 0 || retry) {
            execCount.incrementAndGet();

            builder.setHeader(header);
            builder.setTransactionData(data, StringSerializer.INSTANCE);
            builder.setWriteLocks(writeLocks);
            builder.setReadLocks(readLocks);
            return true;

        } else {
            return false;
        }
    }

    @Override
    public void onCompletion(boolean result) {
        future.complete(result);
    }

    @Override
    public void onApplication() {
        applicationFuture.complete(true);
    }

    @Override
    public void onLockFailure() {
        lockFailureFuture.complete(true);
    }

    public static PartitionLocalLock makeLock(int lock) {
        return new PartitionLocalLock("mock", lock);
    }

    public static class Builder {

        private int partitionId = 0;
        private int header = 0;
        private String data = null;
        private List<PartitionLocalLock> writeLocks = new ArrayList<>();
        private List<PartitionLocalLock> readLocks = new ArrayList<>();
        private boolean retry = true;

        public Builder partitionId(int partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder header(int header) {
            this.header = header;
            return this;
        }

        public Builder data(String data) {
            this.data = data;
            return this;
        }

        public Builder writeLocks(int... locks) {
            for (int lock : locks) {
                this.writeLocks.add(makeLock(lock));
            }
            return this;
        }

        public Builder readLocks(int... locks) {
            for (int lock : locks) {
                this.readLocks.add(makeLock(lock));
            }
            return this;
        }

        public Builder retry(boolean retry) {
            this.retry = retry;
            return this;
        }

        public MockContext build() {
            return new MockContext(partitionId, header, data, writeLocks, readLocks, retry);
        }
    }

}
