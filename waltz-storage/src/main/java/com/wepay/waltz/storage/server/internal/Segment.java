package com.wepay.waltz.storage.server.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.storage.exception.StorageException;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.UUID;
import java.util.zip.CRC32;

public class Segment {

    private static final Logger logger = Logging.getLogger(Segment.class);

    static final int FILE_HEADER_SIZE = 128;
    static final long CHECKPOINT_INTERVAL = 1000;

    static final Comparator<Segment> FIRST_TRANSACTION_ID_COMPARATOR =
        (seg1, seg2) -> Long.compare(seg1.firstTransactionId(), seg2.firstTransactionId());

    // RECORD HEADER: transactionId (8 bytes) + ReqId (16 bytes) + transaction header(4) + data length (4 bytes) + data checksum (4 bytes)
    private static final int TRANSACTION_ID_SIZE = 8;
    private static final int REQ_ID_SIZE = 16;
    private static final int TRANSACTION_HEADER_SIZE = 4;
    private static final int TRANSACTION_DATA_LENGTH_SIZE = 4;
    private static final int TRANSACTION_DATA_CHECKSUM_SIZE = 4;
    private static final int RECORD_HEADER_SIZE
        = TRANSACTION_ID_SIZE + REQ_ID_SIZE + TRANSACTION_HEADER_SIZE + TRANSACTION_DATA_LENGTH_SIZE + TRANSACTION_DATA_CHECKSUM_SIZE;

    // RECORD FOOTER: checksum (4 bytes)
    private static final int RECORD_CHECKSUM_SIZE = 4;
    private static final int RECORD_FOOTER_SIZE = RECORD_CHECKSUM_SIZE;

    private static final int TRANSACTION_ID_POSITION = 0;
    private static final int DATA_LEN_POSITION = TRANSACTION_ID_SIZE + REQ_ID_SIZE + TRANSACTION_HEADER_SIZE;

    private static final int IO_BUF_SIZE = Math.max(RECORD_HEADER_SIZE, RECORD_FOOTER_SIZE);

    private static final int LARGE_BUFFER_SIZE = 1000000;

    private final Path file;
    private final Path indexFile;
    private final SegmentFileHeader header;
    private final long segmentSizeThreshold;
    public final Index index;

    private final byte[] ioBytes = new byte[IO_BUF_SIZE];
    private final ByteBuffer ioBuf = ByteBuffer.wrap(ioBytes);

    private FileChannel channel;
    private long nextTransactionId;
    private volatile long nextOffset;
    private boolean writable = false;
    private boolean closed = false;

    public Segment(UUID key, Path file, Path indexFile, PartitionInfo partitionInfo, long segmentSizeThreshold) throws StorageException {
        this.file = file;
        this.indexFile = indexFile;
        this.segmentSizeThreshold = segmentSizeThreshold;

        try {
            channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException ex) {
            throw new StorageException("failed to open a segment file: " + file.toString(), ex);
        }

        try {
            ByteBuffer headerBuf = ByteBuffer.allocate(FILE_HEADER_SIZE);

            channel.position(0L);

            if (channel.size() < FILE_HEADER_SIZE) {
                logger.error("incomplete header: size=" + channel.size());
            }

            while (headerBuf.remaining() > 0) {
                if (channel.read(headerBuf) < 0) {
                    throw new StorageException("end of file");
                }
            }
            headerBuf.flip();

            header = SegmentFileHeader.readFrom(headerBuf);

            if (!header.key.equals(key)) {
                throw new StorageException("the cluster key does not match");
            }

            if (header.partitionId != partitionInfo.partitionId) {
                throw new StorageException("partition id does not match");
            }

        } catch (Exception ex) {
            try {
                channel.close();
            } catch (IOException error) {
                // ignore
            }
            throw new StorageException("failed to open segment file: " + file.toString(), ex);
        }

        index = new Index(key, indexFile, partitionInfo);

        recover(partitionInfo.getLocalLowWaterMark());
    }

    public void setReadOnly() {
        synchronized (this) {
            writable = false;
        }
    }

    public void setWritable() {
        synchronized (this) {
            writable = true;
        }
    }

    public boolean isWritable() {
        synchronized (this) {
            return writable;
        }
    }

    public boolean isClosed() {
        synchronized (this) {
            return closed;
        }
    }

    public boolean isChannelClosed() {
        synchronized (this) {
            return !channel.isOpen() && index.isChannelClosed();
        }
    }

    void ensureChannelOpened() throws StorageException {
        synchronized (this) {
            index.ensureChannelOpened();

            if (!channel.isOpen()) {
                try {
                    channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
                } catch (IOException ex) {
                    throw new StorageException("failed to open a segment file: " + file.toString(), ex);
                }
            }
        }
    }

    public void closeChannel() {
        synchronized (this) {
            try {
                index.close();
            } catch (IOException ex) {
                logger.error("failed to close the segment index", ex);
            }

            try {
                channel.close();
            } catch (IOException ex) {
                logger.error("failed to close the segment file", ex);
            }
        }
    }

    /**
     * This method scans records and correct index data.
     * Index writes are not flushed to disk for every record write.
     * They are flushed every {@code CHECKPOINT_INTERVAL} to reduce IOs.
     * If a storage node dies, the index data on disk may not be correct.
     * But doing so for the entire segment may be expensive.
     * Therefore, as an optimization, this method finds the most recent stable point that is either the beginning of the segment,
     * the local low-water mark, or the last index flush.
     * Then it gets the record offset from that point and starts scanning records from there.
     * If it finds a record corrupted during scan, the scan is terminated.
     * The segment is truncated to exclude the corrupted record.
     *
     * @param localLowWaterMark local low-water mark
     * @throws StorageException
     */
    private void recover(long localLowWaterMark) throws StorageException {
        try {
            synchronized (this) {
                if (closed) {
                    throw new StorageException("segment closed");
                }

                final long maxTransactionId = index.maxTransactionId();
                long transactionId;
                long offset;
                int size;

                // Find the last checkpoint
                if (maxTransactionId <= localLowWaterMark) {
                    transactionId = maxTransactionId;
                } else {
                    transactionId = ((maxTransactionId - 1) / CHECKPOINT_INTERVAL) * CHECKPOINT_INTERVAL;
                    if (transactionId < localLowWaterMark) {
                        transactionId = localLowWaterMark;
                    }
                }

                if (transactionId <= header.firstTransactionId) {
                    transactionId = header.firstTransactionId;
                    offset = FILE_HEADER_SIZE;

                    if (logger.isDebugEnabled()) {
                        logger.debug("recovering the segment: segment=" + file + " scanFrom=" + transactionId);
                    }

                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("recovering the segment: segment=" + file + " scanFrom=" + transactionId);
                    }

                    offset = index.get(transactionId);
                    if (offset < 0) {
                        throw new StorageException("segment corrupted: index corrupted");
                    } else {
                        index.truncate(transactionId);

                        size = checkRecord(offset, transactionId);
                        if (size < 0) {
                            throw new StorageException("segment corrupted: record corrupted");
                        }

                        offset += size;
                        transactionId++;
                    }
                }

                while (true) {
                    size = checkRecord(offset, transactionId);
                    if (size < 0) {
                        break;
                    } else {
                        index.put(transactionId, offset);
                    }
                    offset += size;
                    transactionId++;
                }

                channel.truncate(offset);
                index.truncate(transactionId - 1);
                flush();

                nextTransactionId = transactionId;
                nextOffset = offset;
            }

        } catch (IOException ex) {
            throw new StorageException("failed to recover segment: segment=" + file, ex);
        }
    }

    public long firstTransactionId() {
        return header.firstTransactionId;
    }

    public long nextTransactionId() {
        synchronized (this) {
            return nextTransactionId;
        }
    }

    public long maxTransactionId() {
        return nextTransactionId() - 1;
    }

    public long size() {
        synchronized (this) {
            return nextOffset;
        }
    }

    public void flush() throws IOException {
        synchronized (this) {
            index.flush();
            channel.force(true);
        }
    }

    public void checksum(CRC32 crc32) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(LARGE_BUFFER_SIZE);

        synchronized (this) {
            long size = channel.size();

            channel.position(FILE_HEADER_SIZE);
            while (channel.position() < size) {
                channel.read(byteBuffer);
                byteBuffer.flip();
                crc32.update(byteBuffer);
                byteBuffer.clear();
            }
        }
        logger.debug("checksum computed: checksum=" + ((int) crc32.getValue()) + " {}", this);
    }

    public void close() {
        synchronized (this) {
            closed = true;
            writable = false;

            try {
                index.close();
            } catch (IOException ex) {
                logger.error("failed to close the segment index", ex);
            }

            try {
                channel.force(true);
            } catch (ClosedChannelException ex) {
                // do nothing.
            } catch (IOException ex) {
                logger.error("failed to flush the segment file", ex);
            }

            try {
                channel.close();
            } catch (IOException ex) {
                logger.error("failed to close the segment file", ex);
            }
        }
    }

    public void delete() throws IOException {
        close();
        try {
            Files.deleteIfExists(file);
        } catch (IOException ex) {
            logger.error("failed to delete file: " + file.toString());
        }
        try {
            Files.deleteIfExists(indexFile);
        } catch (IOException ex) {
            logger.error("failed to delete file: " + indexFile.toString());
        }
    }

    public int append(ArrayList<Record> records, int off) throws StorageException, IOException {
        synchronized (this) {
            if (closed) {
                throw new StorageException("segment closed");
            }

            if (!writable) {
                throw new StorageException("segment not writable");
            }

            channel.position(nextOffset);
            int cumulativeCount = off;
            while (cumulativeCount < records.size()) {
                Record record = records.get(cumulativeCount);
                if (record.transactionId != nextTransactionId) {
                    throw new StorageException("transaction out of order");
                }

                index.put(record.transactionId, nextOffset);

                nextOffset += append(record);
                nextTransactionId++;
                cumulativeCount++;

                if (nextTransactionId % CHECKPOINT_INTERVAL == 0) {
                    flush();
                }

                if (nextOffset > segmentSizeThreshold) {
                    break;
                }
            }
            channel.force(false);

            return cumulativeCount;
        }
    }

    private int append(Record record) throws StorageException, IOException {
        int amount = 0;

        ioBuf.clear();
        ioBuf.putLong(record.transactionId);
        ioBuf.putLong(record.reqId.mostSigBits);
        ioBuf.putLong(record.reqId.leastSigBits);
        ioBuf.putInt(record.header);
        ioBuf.putInt(record.data.length);
        ioBuf.putInt(record.checksum);
        ioBuf.flip();

        while (ioBuf.remaining() > 0) {
            amount += channel.write(ioBuf);
        }

        ioBuf.flip();

        ByteBuffer dataBuf = ByteBuffer.wrap(record.data);

        while (dataBuf.remaining() > 0) {
            amount += channel.write(dataBuf);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(ioBuf);
        crc32.update(record.data);

        ioBuf.clear();
        ioBuf.putInt((int) crc32.getValue());
        ioBuf.flip();

        while (ioBuf.remaining() > 0) {
            amount += channel.write(ioBuf);
        }

        return amount;
    }

    public RecordHeader getRecordHeader(long transactionId) throws StorageException, IOException {
        synchronized (this) {
            if (closed) {
                throw new StorageException("segment closed");
            }

            long offset = index.get(transactionId);
            if (offset < 0) {
                return null;
            }

            channel.position(offset);
            ioBuf.clear();
            ioBuf.limit(RECORD_HEADER_SIZE - 8); // exclude data length and data checksum
            while (ioBuf.remaining() > 0) {
                if (channel.read(ioBuf) < 0) {
                    throw new StorageException("end of file");
                }
            }
            ioBuf.flip();

            if (ioBuf.getLong() != transactionId) {
                throw new StorageException("transaction id mismatch");
            }

            ReqId reqId = new ReqId(ioBuf.getLong(), ioBuf.getLong());
            int transactionHeader = ioBuf.getInt();

            return new RecordHeader(transactionId, reqId, transactionHeader);
        }
    }

    public Record getRecord(long transactionId) throws StorageException, IOException {
        synchronized (this) {
            if (closed) {
                throw new StorageException("segment closed");
            }

            long offset = index.get(transactionId);
            if (offset < 0) {
                return null; // not found
            }

            ioBuf.clear();
            ioBuf.limit(RECORD_HEADER_SIZE);
            channel.position(offset);
            while (ioBuf.remaining() > 0) {
                if (channel.read(ioBuf) < 0) {
                    throw new StorageException("end of file");
                }
            }
            ioBuf.flip();

            if (ioBuf.getLong() != transactionId) {
                throw new StorageException("transaction id mismatch");
            }

            ReqId reqId = new ReqId(ioBuf.getLong(), ioBuf.getLong());
            int transactionHeader = ioBuf.getInt();
            int dataLen = ioBuf.getInt();
            int dataChecksum = ioBuf.getInt();

            byte[] dataBytes = new byte[dataLen];
            ByteBuffer dataBuf = ByteBuffer.wrap(dataBytes);
            while (dataBuf.remaining() > 0) {
                if (channel.read(dataBuf) < 0) {
                    throw new StorageException("end of file");
                }
            }

            return new Record(transactionId, reqId, transactionHeader, dataBytes, dataChecksum);
        }
    }

    // check the record at the offset. If valid, it returns the size, otherwise it returns -1.
    public int checkRecord(long offset, long transactionId) throws IOException {
        synchronized (this) {
            // Read the header
            ioBuf.clear();
            ioBuf.limit(RECORD_HEADER_SIZE);
            channel.position(offset);
            while (ioBuf.remaining() > 0) {
                if (channel.read(ioBuf) < 0) {
                    return -1;
                }
            }
            ioBuf.flip();

            CRC32 crc32 = new CRC32();
            crc32.update(ioBuf);

            if (ioBuf.getLong(TRANSACTION_ID_POSITION) != transactionId) {
                return -1;
            }

            // Read the data
            int dataLen = ioBuf.getInt(DATA_LEN_POSITION);
            ByteBuffer dataBuf = ByteBuffer.allocate(dataLen);
            while (dataBuf.remaining() > 0) {
                if (channel.read(dataBuf) < 0) {
                    return -1;
                }
            }
            dataBuf.flip();
            crc32.update(dataBuf);

            ioBuf.clear();
            ioBuf.limit(4);
            while (ioBuf.remaining() > 0) {
                if (channel.read(ioBuf) < 0) {
                    return -1;
                }
            }
            ioBuf.flip();
            int checksum = ioBuf.getInt();

            return checksum == (int) crc32.getValue() ? RECORD_HEADER_SIZE + dataLen + RECORD_FOOTER_SIZE : -1;
        }
    }

    // Truncates transactions after the given transaction id
    public void truncate(long transactionId) throws StorageException, IOException {
        synchronized (this) {
            if (closed) {
                throw new StorageException("segment closed");
            }

            if (transactionId + 1 == nextTransactionId) {
                return;
            }

            if (transactionId  + 1 < header.firstTransactionId || transactionId + 1 > nextTransactionId) {
                throw new StorageException("transaction id out of range: file=" + file.toString());
            }

            long offset = index.get(transactionId + 1);
            if (offset < 0) {
                throw new StorageException("corrupted");
            }
            index.truncate(transactionId);

            channel.truncate(offset);
            channel.force(true);

            nextOffset = offset;
            nextTransactionId = transactionId + 1;
        }
    }

    public String toString() {
        return "Segment[partitionId=" + header.partitionId + " file=" + file.toString() + "]";
    }

    public static void create(UUID key, Path segmentFile, Path indexFile, int partitionId, long firstTransaction) throws StorageException, IOException {
        create(key, indexFile, partitionId, firstTransaction);
        create(key, segmentFile, partitionId, firstTransaction);
    }

    private static void create(UUID key, Path file, int partitionId, long firstTransactionId) throws StorageException, IOException {
        try {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                if (channel.size() == 0) {
                    FileLock lock = channel.tryLock(0, FILE_HEADER_SIZE, false);

                    if (lock == null) {
                        throw new StorageException("failed to acquire the lock");
                    }

                    try {
                        SegmentFileHeader header = new SegmentFileHeader(key, partitionId, firstTransactionId);
                        ByteBuffer headerBuf = ByteBuffer.allocate(FILE_HEADER_SIZE);

                        header.writeTo(headerBuf);
                        while (headerBuf.remaining() > 0) {
                            headerBuf.put((byte) 0);
                        }
                        headerBuf.flip();

                        channel.position(0L);
                        channel.write(headerBuf);
                        channel.force(true);

                    } finally {
                        try {
                            lock.release();
                        } catch (IOException ex) {
                            // ignore
                        }
                    }

                } else {
                    throw new StorageException("segment file not empty: " + file.toString());
                }
            } finally {
                try {
                    channel.close();
                } catch (IOException ex) {
                    // ignore
                }
            }
        } catch (IOException ex) {
            throw new StorageException("failed to create segment file: " + file.toString(), ex);
        }
    }

    public static class Index {

        private static final int OFFSET_SIZE = 8;

        private final Path file;
        private final SegmentFileHeader header;
        private final ByteBuffer offsetBuf = ByteBuffer.allocate(8);

        private FileChannel channel;

        Index(UUID key, Path file, PartitionInfo partitionInfo) throws StorageException {
            this.file = file;

            try {
                channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException ex) {
                throw new StorageException("failed to open segment file: " + file.toString(), ex);
            }

            try {
                ByteBuffer headerBuf = ByteBuffer.allocate(FILE_HEADER_SIZE);

                channel.position(0L);
                while (headerBuf.remaining() > 0) {
                    if (channel.read(headerBuf) < 0) {
                        throw new StorageException("end of file");
                    }
                }
                headerBuf.flip();

                header = SegmentFileHeader.readFrom(headerBuf);

                if (!header.key.equals(key)) {
                    throw new StorageException("the cluster key does not match");
                }

                if (header.partitionId != partitionInfo.partitionId) {
                    throw new StorageException("partition id does not match");
                }

            } catch (IOException ex) {
                try {
                    channel.close();
                } catch (IOException error) {
                    // ignore
                }
                throw new StorageException("failed to open segment file: " + file.toString(), ex);
            }
        }

        public long get(long transactionId) throws StorageException, IOException {
            synchronized (this) {
                long position = FILE_HEADER_SIZE + OFFSET_SIZE * (transactionId - header.firstTransactionId);

                if (position < FILE_HEADER_SIZE) {
                    throw new StorageException("illegal record position:" + position);
                }

                channel.position(FILE_HEADER_SIZE + OFFSET_SIZE * (transactionId - header.firstTransactionId));
                offsetBuf.clear();
                while (offsetBuf.remaining() > 0) {
                    if (channel.read(offsetBuf) < 0) {
                        return -1L;
                    }
                }
                offsetBuf.flip();
                return offsetBuf.getLong();
            }
        }

        void put(long transactionId, long offset) throws IOException {
            synchronized (this) {
                offsetBuf.clear();
                offsetBuf.putLong(offset);
                offsetBuf.flip();
                channel.position(FILE_HEADER_SIZE + OFFSET_SIZE * (transactionId - header.firstTransactionId));
                while (offsetBuf.remaining() > 0) {
                    channel.write(offsetBuf);
                }
            }
        }

        // Truncates transactions after the given transaction id
        void truncate(long transactionId) throws IOException {
            synchronized (this) {
                long size = FILE_HEADER_SIZE + OFFSET_SIZE * (transactionId + 1 - header.firstTransactionId);
                if (channel.size() > size) {
                    channel.truncate(size);
                    channel.force(true);
                }
            }
        }

        long maxTransactionId() throws IOException {
            synchronized (this) {
                long numTransactionIds = (channel.size() - FILE_HEADER_SIZE) / OFFSET_SIZE;

                return header.firstTransactionId + numTransactionIds - 1;
            }
        }

        void flush() throws IOException {
            synchronized (this) {
                channel.force(true);
            }
        }

        void close() throws IOException {
            synchronized (this) {
                try {
                    channel.force(true);
                } catch (IOException ex) {
                    // ignore
                }
                channel.close();
            }
        }

        void ensureChannelOpened() throws StorageException {
            synchronized (this) {
                if (!channel.isOpen()) {
                    try {
                        channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
                    } catch (IOException ex) {
                        throw new StorageException("failed to open segment file: " + file.toString(), ex);
                    }
                }
            }
        }

        public boolean isChannelClosed() {
            synchronized (this) {
                return !channel.isOpen();
            }
        }

        public SegmentFileHeader getHeader() {
            return header;
        }
    }

}
