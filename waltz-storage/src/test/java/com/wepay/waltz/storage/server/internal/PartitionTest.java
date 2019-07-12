package com.wepay.waltz.storage.server.internal;

import com.wepay.riff.network.Message;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.storage.common.message.AppendRequest;
import com.wepay.waltz.storage.common.message.SuccessResponse;
import com.wepay.waltz.test.util.ClientUtil;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionTest {

    private static final int PARTITION_ID = 0;

    private File dir;
    private Path partitionDir;

    @Before
    public void setup() throws Exception {
        dir = Files.createTempDirectory("tempDir-").toFile();
        Path dirPath = FileSystems.getDefault().getPath(dir.getPath());
        partitionDir = dirPath.resolve(Integer.toString(PARTITION_ID));

        if (!Files.exists(partitionDir)) {
            Files.createDirectory(partitionDir);
        }
    }

    @After
    public void teardown() {
        Utils.removeDirectory(dir);
    }

    /**
     * This method sets the maximum capacity of the LRUCache to 1 and creates 3 Segments by adding records to the partition.
     * Only readable Segments are added to the Cache. Evicted segment from the cache is verified.
     */
    @Test
    public void testPartitionLruCache() throws Exception {
        int segmentCacheCapacity = 1;
        long segmentSizeThreshold = 400L;

        ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
        PartitionInfo partitionInfo = new PartitionInfo(byteBuffer, 0, null, PARTITION_ID, true);
        partitionInfo.setFlags(PartitionInfo.Flags.PARTITION_IS_ASSIGNED | PartitionInfo.Flags.PARTITION_IS_AVAILABLE);

        TestPartitionClass partition = new TestPartitionClass(UUID.randomUUID(), partitionDir, partitionInfo, segmentSizeThreshold, segmentCacheCapacity);

        ArrayList<Record> records = ClientUtil.makeRecords(0, 21);
        AppendRequest msg = new AppendRequest(-1, 0, PARTITION_ID, records);
        partition.open();

        partition.receiveMessage(msg, null);

        int numTry = 10;
        while ((numTry != 0) && (partition.evictedEntrylist.size() == 0)) {
            Thread.sleep(200);
            numTry--;
        }

        // 3 Segments would have been created for this Partition based on the records.
        // As the max capacity of LRUCache is just 1, one readable segment would have been evicted from the Cache.
        assertEquals(partition.evictedEntrylist.size(), 1);

        // Verify clean-up function.
        Segment evictedSegment = partition.evictedEntrylist.get(0);
        assertTrue(evictedSegment.isChannelClosed());
    }

    /**
     * This is a mini read/write stress test for a partition.
     */
    @Test
    public void testConcurrentReadsAndWrites() throws Exception {
        int segmentCacheCapacity = 100;
        long segmentSizeThreshold = 1000L;

        ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
        PartitionInfo partitionInfo = new PartitionInfo(byteBuffer, 0, null, PARTITION_ID, true);
        partitionInfo.setFlags(PartitionInfo.Flags.PARTITION_IS_ASSIGNED | PartitionInfo.Flags.PARTITION_IS_AVAILABLE);

        TestPartitionClass partition = new TestPartitionClass(UUID.randomUUID(), partitionDir, partitionInfo, segmentSizeThreshold, segmentCacheCapacity);
        partition.open();

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        WriteThread writeThread = new WriteThread(partition, future);
        writeThread.start();

        ReadThread[] readThreads = new ReadThread[1];
        for (int i = 0; i < readThreads.length; i++) {
            readThreads[i] = new ReadThread(partition, writeThread, future);
        }

        for (ReadThread readThread : readThreads) {
            readThread.start();
        }

        for (int i = 0; i < 10 && !future.isDone(); i++) {
            Uninterruptibly.sleep(1000);
        }
        future.complete(true);

        writeThread.join();
        for (ReadThread readThread : readThreads) {
            readThread.join();
        }

        try {
            future.get();

        } catch (ExecutionException ex) {
            ex.getCause().printStackTrace();
            fail(ex.getCause().getMessage());
        }

        assertTrue(partition.evictedEntrylist.size() > 1);
    }

    public static class TestPartitionClass extends Partition {

        public final List<Segment> evictedEntrylist;

        TestPartitionClass(UUID key, Path directory, PartitionInfo partitionInfo, long segmentSizeThreshold, int segmentCacheCapacity) {
            super(key, directory, partitionInfo, segmentSizeThreshold, segmentCacheCapacity);
            this.evictedEntrylist = new ArrayList<>();
        }

        @Override
        protected void cleanupFunc(Map.Entry<Segment, Object> entry) {
            super.cleanupFunc(entry);
            evictedEntrylist.add(entry.getKey());
        }

    }

    private static class WriteThread extends Thread {

        private final Random rand = new Random();
        private final Partition partition;
        private final AtomicLong nextTransactionId = new AtomicLong(0L);
        private final CompletableFuture<Boolean> future;

        WriteThread(Partition partition, CompletableFuture<Boolean> future) {
            this.partition = partition;
            this.future = future;
        }

        long nextTransactionId() {
            return nextTransactionId.get();
        }

        @Override
        public void run() {
            while (!future.isDone()) {
                long startTransactionId = nextTransactionId.get();
                long endTransactionId = nextTransactionId.get() + rand.nextInt(100);

                if (endTransactionId >= Integer.MAX_VALUE) {
                    // Too many
                    break;
                }

                ArrayList<Record> records = ClientUtil.makeRecords(startTransactionId, endTransactionId);
                AppendRequest request = new AppendRequest(-1, 0, PARTITION_ID, records);

                CompletableFuture<Message> writeFuture = new CompletableFuture<>();
                partition.receiveMessage(request, new PartitionClient() {
                    @Override
                    public boolean sendMessage(Message msg, boolean flush) {
                        writeFuture.complete(msg);
                        return true;
                    }
                });

                Message response;

                while (true) {
                    try {
                        response = writeFuture.get();
                        break;

                    } catch (InterruptedException ex) {
                        //
                    } catch (ExecutionException ex) {
                        future.completeExceptionally(ex.getCause());
                        return;
                    }
                }

                if (response instanceof SuccessResponse) {
                    nextTransactionId.set(endTransactionId);

                } else {
                    future.completeExceptionally(new Exception("write failed"));
                    return;
                }
            }
        }

    }

    private static class ReadThread extends Thread {

        private final Random rand = new Random();
        private final Partition partition;
        private final WriteThread writeThread;
        private final CompletableFuture<Boolean> future;

        ReadThread(Partition partition, WriteThread writeThread, CompletableFuture<Boolean> future) {
            this.partition = partition;
            this.writeThread = writeThread;
            this.future = future;
        }

        @Override
        public void run() {
            while (!future.isDone()) {
                long startTransactionId = (long) rand.nextInt((int) writeThread.nextTransactionId() + 1);
                int numRecords = rand.nextInt(100);
                boolean mayReturnEmpty =
                    numRecords == 0 || startTransactionId == writeThread.nextTransactionId();

                try {
                    ArrayList<Record> records = partition.getRecords(startTransactionId, numRecords);

                    if (records.size() > 0) {
                        int i = 0;
                        for (Record record : records) {
                            if (startTransactionId + i != record.transactionId) {
                                String msg = String.format(
                                    "wrong transactionId: expected=%d actual=%d record=%d/%d",
                                    startTransactionId + i,
                                    record.transactionId,
                                    i,
                                    records.size()
                                );
                                future.completeExceptionally(new Exception(msg));
                                return;
                            }

                            byte[] expectedData = ClientUtil.generateData(startTransactionId + i);

                            if (!Arrays.equals(expectedData, record.data)) {
                                String msg = String.format(
                                    "wrong data: record=%d/%d", i, records.size()
                                );
                                future.completeExceptionally(new Exception(msg));
                                return;

                            }

                            CRC32 crc32 = new CRC32();
                            crc32.update(record.data);

                            if ((int) crc32.getValue() != record.checksum) {
                                String msg = String.format("wrong data checksum: record=%d/%d", i, records.size());
                                future.completeExceptionally(new Exception(msg));
                                return;
                            }

                            i++;
                        }
                    } else {
                        if (!mayReturnEmpty) {
                            future.completeExceptionally(new Exception("empty records list returned"));
                            return;
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

    }

}
