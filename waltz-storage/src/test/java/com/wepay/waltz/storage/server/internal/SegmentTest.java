package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.storage.exception.StorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentTest {

    private Random rand = new Random();
    private Path dir;
    private Path controlFilePath;
    private Path segmentPath;
    private Path indexPath;
    private UUID key;
    private int numPartitions;
    private ControlFile controlFile;

    @Before
    public void setup() throws Exception {
        dir = Files.createTempDirectory("segment-test");
        controlFilePath = dir.resolve(ControlFile.FILE_NAME);
        segmentPath = dir.resolve("test.seg");
        indexPath = dir.resolve("test.idx");

        key = UUID.randomUUID();
        numPartitions = 3;
        controlFile = new ControlFile(key, controlFilePath, numPartitions, true);
        controlFile.flush();
    }

    @After
    public void teardown() {
        controlFile.close();
        delete(segmentPath);
        delete(indexPath);
        delete(controlFilePath);
        delete(dir);
    }

    @Test
    public void testKey() throws Exception {
        Path wrongSegmentPath = dir.resolve("wrong.seg");
        Path wrongIndexPath = dir.resolve("wrong.idx");

        try {
            UUID wrongKey = UUID.randomUUID();
            long segmentSizeThreshold = 1000;

            Segment.create(key, segmentPath, indexPath, 0, 0L);
            Segment.create(wrongKey, wrongSegmentPath, wrongIndexPath, 0, 0L);

            try {
                new Segment(key, wrongSegmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
                fail();
            } catch (StorageException ex) {
                // OK
            }
            try {
                new Segment(key, segmentPath, wrongIndexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
                fail();
            } catch (StorageException ex) {
                // OK
            }

            try {
                new Segment(key, wrongSegmentPath, wrongIndexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
                fail();
            } catch (StorageException ex) {
                // OK
            }

            try {
                Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
                segment.delete();
            } catch (StorageException ex) {
                fail();
            }

            assertFalse(Files.exists(segmentPath));
            assertFalse(Files.exists(indexPath));

        } finally {
            delete(wrongSegmentPath);
            delete(wrongIndexPath);
        }
    }

    private void delete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ex) {
            // ignore
        }
    }

    @Test
    public void testTransactionOutOfOrder() throws Exception {
        long segmentSizeThreshold = 1000;
        long transactionId = rand.nextInt(10);

        Segment.create(key, segmentPath, indexPath, 0, transactionId);
        Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
        segment.setWritable();

        assertEquals(transactionId, segment.firstTransactionId());
        assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

        for (int i = 0; i < 3; i++) {
            ArrayList<Record> records = new ArrayList<>();
            byte[] data = new byte[100];
            records.add(new Record(transactionId++, reqId(), 0, data, Utils.checksum(data)));
            segment.append(records, 0);
        }

        try {
            transactionId--; // duplicate id
            ArrayList<Record> records = new ArrayList<>();
            byte[] data = new byte[100];
            records.add(new Record(transactionId, reqId(), 0, data, Utils.checksum(data)));
            segment.append(records, 0);
            fail();

        } catch (StorageException ex) {
            // OK
        }
    }

    @Test
    public void testThreshold() throws Exception {
        long segmentSizeThreshold = 1000;
        long transactionId = rand.nextInt(10);

        Segment.create(key, segmentPath, indexPath, 0, transactionId);
        Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
        segment.setWritable();

        assertEquals(transactionId, segment.firstTransactionId());
        assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

        while (segment.size() < segmentSizeThreshold) {
            ArrayList<Record> records = new ArrayList<>();
            byte[] data = new byte[100];
            records.add(new Record(transactionId++, reqId(), 0, data, Utils.checksum(data)));

            long currentSize = segment.size();
            int count = segment.append(records, 0);

            if (currentSize <= segmentSizeThreshold) {
                assertEquals(1, count);
            } else {
                assertEquals(0, count);
                break;
            }
        }

        assertTrue(segment.size() >= segmentSizeThreshold);
    }

    @Test
    public void testTruncate() throws Exception {
        long startTransactionId = rand.nextInt(1000);
        long segmentSizeThreshold = 1000;

        Segment.create(key, segmentPath, indexPath, 0, startTransactionId);
        Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
        segment.setWritable();

        assertEquals(startTransactionId, segment.firstTransactionId());
        assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

        ArrayList<Long> transactionnId = new ArrayList<>();
        ArrayList<Long> sizeAfterAppend = new ArrayList<>();

        // fill up the segment
        long count = 0;
        while (segment.size() < segmentSizeThreshold) {
            ArrayList<Record> records = new ArrayList<>();
            byte[] data = new byte[100];
            records.add(new Record(startTransactionId + count, reqId(), 0, data, Utils.checksum(data)));

            if (segment.append(records, 0) == 0) {
                break;
            }

            transactionnId.add(startTransactionId + count);
            sizeAfterAppend.add(segment.size());
            count++;
        }

        segment.truncate(segment.maxTransactionId());

        for (int i = sizeAfterAppend.size() - 1; i >= 0; i--) {
            segment.truncate(transactionnId.get(i));
            assertEquals(sizeAfterAppend.get(i), (Long) segment.size());
        }

        segment.truncate(startTransactionId - 1L);
        assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

        segment.close();
    }

    @Test
    public void testTruncateOutOfRange() throws Exception {
        long startTransactionId = rand.nextInt(1000);
        long segmentSizeThreshold = 1000;
        int numMaxTransactions = rand.nextInt(10);

        Segment.create(key, segmentPath, indexPath, 0, startTransactionId);
        Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
        segment.setWritable();

        assertEquals(startTransactionId, segment.firstTransactionId());
        assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

        ArrayList<Record> records = new ArrayList<>();
        byte[] data = new byte[100];
        for (int i = 0; i < numMaxTransactions; i++) {
            records.add(new Record(startTransactionId + i, reqId(), 0, data, Utils.checksum(data)));
        }

        int count = segment.append(records, 0);

        assertEquals(startTransactionId + count - 1, segment.maxTransactionId());

        try {
            segment.truncate(startTransactionId - 2);
            fail();
        } catch (StorageException ex) {
            // ignore
        }

        try {
            segment.truncate(segment.maxTransactionId() + 1);
            fail();
        } catch (StorageException ex) {
            // ignore
        }

        segment.close();
    }

    @Test
    public void testIndexRecovery() throws Exception {
        for (int n = 0; n < 100; n++) {
            controlFile.getPartitionInfo(0).reset();

            long startTransactionId = rand.nextInt(1000);
            long segmentSizeThreshold = 1000000;
            int numMaxTransactions = rand.nextInt(100);
            int numHalfTransactions = numMaxTransactions / 2;

            Segment.create(key, segmentPath, indexPath, 0, startTransactionId);
            Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
            segment.setWritable();

            assertEquals(startTransactionId, segment.firstTransactionId());
            assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

            ArrayList<Record> records1 = new ArrayList<>();
            ArrayList<Record> records2 = new ArrayList<>();
            byte[] data = new byte[100];
            for (int i = 0; i < numMaxTransactions; i++) {
                if (i < numHalfTransactions) {
                    records1.add(new Record(startTransactionId + i, reqId(), 0, data, Utils.checksum(data)));
                } else {
                    records2.add(new Record(startTransactionId + i, reqId(), 0, data, Utils.checksum(data)));
                }
            }

            FileChannel channel = FileChannel.open(indexPath, StandardOpenOption.READ, StandardOpenOption.WRITE);

            segment.append(records1, 0);
            segment.flush();

            long halfPoint = channel.size();
            long lowWaterMark = startTransactionId + numHalfTransactions - 1;
            controlFile.getPartitionInfo(0).setLowWaterMark(1, lowWaterMark, lowWaterMark);

            segment.append(records2, 0);
            segment.flush();

            segment.close();

            channel.truncate(halfPoint);
            channel.close();

            segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);

            for (int i = 0; i < numMaxTransactions; i++) {
                assertNotNull(segment.getRecordHeader(startTransactionId + i));
            }

            segment.close();
            delete(segmentPath);
            delete(indexPath);
        }
    }

    @Test
    public void testIncompleteWrite() throws Exception {
        for (int n = 0; n < 500; n++) {
            long startTransactionId = rand.nextInt((int) Segment.CHECKPOINT_INTERVAL + 1);
            long segmentSizeThreshold = 1000000;
            int numMaxTransactions = rand.nextInt(100) + 1;

            Segment.create(key, segmentPath, indexPath, 0, startTransactionId);
            Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
            segment.setWritable();

            assertEquals(startTransactionId, segment.firstTransactionId());
            assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

            ArrayList<Record> records = new ArrayList<>();
            byte[] data = new byte[100];
            for (int i = 0; i < numMaxTransactions; i++) {
                records.add(new Record(startTransactionId + i, reqId(), 0, data, Utils.checksum(data)));
            }

            segment.append(records, 0);
            assertEquals(startTransactionId + numMaxTransactions - 1, segment.maxTransactionId());

            segment.flush();
            segment.close();

            // truncate the last record to emulate an incomplete write
            FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
            channel.truncate(channel.size() - 10);
            channel.close();

            segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
            segment.setWritable();

            assertEquals(startTransactionId + numMaxTransactions - 2, segment.maxTransactionId());

            segment.close();
            delete(segmentPath);
            delete(indexPath);
        }
    }

    @Test
    public void testDirtyWrite() throws Exception {
        for (int n = 0; n < 500; n++) {
            long startTransactionId = rand.nextInt((int) Segment.CHECKPOINT_INTERVAL + 1);
            long segmentSizeThreshold = 1000000;
            int maxTransactions = rand.nextInt(100) + 1;

            Segment.create(key, segmentPath, indexPath, 0, startTransactionId);
            Segment segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
            segment.setWritable();

            assertEquals(startTransactionId, segment.firstTransactionId());
            assertEquals(Segment.FILE_HEADER_SIZE, segment.size());

            ArrayList<Record> records = new ArrayList<>();
            byte[] data = new byte[100];
            for (int i = 0; i < maxTransactions; i++) {
                records.add(new Record(startTransactionId + i, reqId(), 0, data, Utils.checksum(data)));
            }

            segment.append(records, 0);
            assertEquals(startTransactionId + maxTransactions - 1, segment.maxTransactionId());

            segment.flush();
            segment.close();

            // destroy the checksum of the last record to emulate a dirty write
            ByteBuffer buf = ByteBuffer.allocate(4);
            FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long position = channel.size() - 4;
            while (buf.remaining() > 0) {
                if (channel.read(buf, position) < 0) {
                    throw new EOFException();
                }
            }
            buf.put(0, (byte) ~buf.get(0));
            buf.flip();
            channel.write(buf, position);
            channel.close();

            segment = new Segment(key, segmentPath, indexPath, controlFile.getPartitionInfo(0), segmentSizeThreshold);
            segment.setWritable();

            if (startTransactionId + maxTransactions - 2 != segment.maxTransactionId()) {
                assertEquals(startTransactionId + maxTransactions - 2, segment.maxTransactionId());
            }

            segment.close();
            delete(segmentPath);
            delete(indexPath);
        }
    }

    private ReqId reqId() {
        return new ReqId(rand.nextLong(), rand.nextLong());
    }

}
