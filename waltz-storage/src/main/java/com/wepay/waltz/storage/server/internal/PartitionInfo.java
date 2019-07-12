package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.ConcurrentUpdateException;
import com.wepay.waltz.storage.exception.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PartitionInfo is a wrapper for {@link PartitionInfoStruct} that maintains two copies of the struct: current and next.
 * Two partition info structs are updated alternately when a new session is started and immediately flushed to the disk.
 * The checksum is checked when a partition of opened. Since there is no guarantee of atomicity of I/O, it is possible
 * that an update is not completely rewritten to the file when a fault occurs during I/O. If one of the structs has a
 * checksum error, we ignore it and use the other struct, which means we rollback the partition. We assume at least one
 * of them is always valid. If both current and next are valid structs, PartitionInfo will use whichever struct has a
 * higher session ID. If neither of structs is valid, we fail to open the partition.
 */
class PartitionInfo {
    private static final int PARTITION_ID_START = 0;
    private static final int PARTITION_ID_SIZE = 4;

    private static final int STRUCT1_POSITION = PARTITION_ID_SIZE;
    private static final int STRUCT2_POSITION = STRUCT1_POSITION + PartitionInfoStruct.SIZE;

    static final int SIZE = PARTITION_ID_SIZE + PartitionInfoStruct.SIZE * 2;

    public final int partitionId;

    private final FileChannel channel;

    private PartitionInfoStruct current;
    private PartitionInfoStruct next;

    private AtomicInteger sequenceNumber;

    PartitionInfo(ByteBuffer byteBuffer, int position, FileChannel channel, int partitionId, boolean reformat) throws StorageException {
        this.partitionId = partitionId;
        this.channel = channel;

        if (reformat) {
            sequenceNumber = null;
            byteBuffer.putInt(position + PARTITION_ID_START, partitionId);
        }

        PartitionInfoStruct struct1 = new PartitionInfoStruct(byteBuffer, position + STRUCT1_POSITION, reformat);
        PartitionInfoStruct struct2 = new PartitionInfoStruct(byteBuffer, position + STRUCT2_POSITION, reformat);

        if (struct1.isValid()) {
            if (struct2.isValid()) {
                this.current = max(struct1, struct2);
                this.next = min(struct1, struct2);
            } else {
                this.current = struct1;
                this.next = struct2;
            }
        } else {
            if (struct2.isValid()) {
                this.current = struct2;
                this.next = struct1;
            } else {
                throw new StorageException("partition info corrupted");
            }
        }

        this.sequenceNumber = new AtomicInteger(this.current.sequenceNumber());
    }

    PartitionInfoSnapshot getSnapshot() {
        synchronized (this) {
            return new PartitionInfoSnapshot(
                    partitionId,
                    current.sessionId,
                    current.lowWaterMark,
                    current.localLowWaterMark,
                    current.flags
            );
        }

    }

    long sessionId() {
        synchronized (this) {
            return current.sessionId();
        }
    }

    SessionInfo getLastSessionInfo() {
        synchronized (this) {
            return new SessionInfo(current.sessionId(), current.lowWaterMark());
        }
    }

    long getLowWaterMark() {
        synchronized (this) {
            return current.lowWaterMark();
        }
    }

    long getLocalLowWaterMark() {
        synchronized (this) {
            return current.localLowWaterMark();
        }
    }

    int getFlags() {
        synchronized (this) {
            return current.flags();
        }
    }

    int getSequenceNumber() {
        synchronized (this) {
            return current.sequenceNumber();
        }
    }

    void setLowWaterMark(long sessionId, long lowWaterMark, long localLowWaterMark) throws ConcurrentUpdateException, IOException, StorageException {
        synchronized (this) {
            if (current.sessionId() <= sessionId) {
                if (lowWaterMark < current.lowWaterMark()) {
                    throw new StorageException("unable to set low watermark to value " + lowWaterMark
                            + " because it's less than current low watermark " + current.lowWaterMark());
                } else if (localLowWaterMark < current.localLowWaterMark()) {
                    throw new StorageException("unable to set local low watermark to value " + localLowWaterMark
                            + " because it's less than current local low watermark " + current.localLowWaterMark());
                }

                next.update(sessionId, lowWaterMark, localLowWaterMark, current.flags());

                swap();
                flush();
            } else {
                throw new ConcurrentUpdateException("session id has changed to " + current.sessionId()
                        + ", but low watermark request contains session id " + sessionId);
            }
        }
    }

    void setFlags(int flags) throws ConcurrentUpdateException, IOException {
        synchronized (this) {
            next.update(current.sessionId(), current.lowWaterMark(), current.localLowWaterMark(), flags);

            swap();
            flush();
        }
    }

    void setFlag(int flag, boolean toggled) throws ConcurrentUpdateException, IOException {
        synchronized (this) {
            if (toggled) {
                // Force the flag bit to true by ORing it with the flag
                setFlags(getFlags() | flag);
            } else {
                // Force the flag bit to false by inverting the flag and ANDing it with the current flags
                setFlags(getFlags() & ~flag);
            }
        }
    }

    void reset() throws IOException {
        synchronized (this) {
            sequenceNumber = null;

            current.create();
            next.create();

            flush();
        }
    }

    // for testing
    void invalidate() {
        synchronized (this) {
            current.invalidate();
        }
    }

    private PartitionInfoStruct max(PartitionInfoStruct struct1, PartitionInfoStruct struct2) throws StorageException {
        if (struct1.sequenceNumber() > struct2.sequenceNumber()) {
            return struct1;
        } else if (struct1.sequenceNumber() < struct2.sequenceNumber()) {
            return struct2;
        } else {
            throw new StorageException("Cannot determine max PartitionInfoStruct: same sequence number found");
        }
    }

    private PartitionInfoStruct min(PartitionInfoStruct struct1, PartitionInfoStruct struct2) throws StorageException {
        if (struct1.sequenceNumber() < struct2.sequenceNumber()) {
            return struct1;
        } else if (struct1.sequenceNumber() > struct2.sequenceNumber()) {
            return struct2;
        } else {
            throw new StorageException("Cannot determine min PartitionInfoStruct: same sequence number found");
        }
    }

    /**
     * Swaps the current and next {@link PartitionInfoStruct} objects. No validation is done when the swap occurs.
     */
    private void swap() {
        PartitionInfoStruct struct = current;
        current = next;
        next = struct;
    }

    /**
     * Flush the file channel for the control file.
     */
    private void flush() throws IOException {
        if (channel != null) {
            channel.force(false);
        }
    }

    public String toString() {
        synchronized (this) {
            return "partitionId=" + partitionId + " session=[" + current + "]";
        }
    }

    /**
     * A partition info struct records the session id, the low-water mark which is the smallest valid transaction id of
     * the partition in the cluster, the local low-water mark which is the smallest valid transaction id of the
     * partition in the storage (the local low-water mark can be smaller than the low-water mark), flags, and the
     * checksum of the struct.
     */
    protected class PartitionInfoStruct {

        static final int SESSION_ID_SIZE = 8;
        static final int LOW_WATER_MARK_SIZE = 8;
        static final int LOCAL_LOW_WATER_MARK_SIZE = 8;
        static final int FLAGS_SIZE = 4;
        static final int SEQUENCE_NUMBER_SIZE = 4;
        static final int CHECKSUM_SIZE = 4;

        static final int SESSION_ID_POSITION = 0;
        static final int LOW_WATER_MARK_POSITION = SESSION_ID_POSITION + SESSION_ID_SIZE;
        static final int LOCAL_LOW_WATER_MARK_POSITION = LOW_WATER_MARK_POSITION + LOW_WATER_MARK_SIZE;
        static final int FLAGS_POSITION = LOCAL_LOW_WATER_MARK_POSITION + LOCAL_LOW_WATER_MARK_SIZE;
        static final int SEQUENCE_NUMBER_POSITION = FLAGS_POSITION + FLAGS_SIZE;
        static final int CHECKSUM_POSITION = SEQUENCE_NUMBER_POSITION + SEQUENCE_NUMBER_SIZE;

        static final int SIZE = CHECKSUM_POSITION + CHECKSUM_SIZE;

        private final ByteBuffer byteBuffer;
        private final int position;

        private long sessionId;
        private long lowWaterMark;
        private long localLowWaterMark;
        private int flags;
        private int structSequenceNumber;

        PartitionInfoStruct(ByteBuffer byteBuffer, int position, boolean reformat) throws StorageException {
            this.byteBuffer = byteBuffer;
            this.position = position;

            if (reformat) {
                create();
            } else {
                this.sessionId = byteBuffer.getLong(position + SESSION_ID_POSITION);
                this.lowWaterMark = byteBuffer.getLong(position + LOW_WATER_MARK_POSITION);
                this.localLowWaterMark = byteBuffer.getLong(position + LOCAL_LOW_WATER_MARK_POSITION);
                this.flags = byteBuffer.getInt(position + FLAGS_POSITION);
                this.structSequenceNumber = byteBuffer.getInt(position + SEQUENCE_NUMBER_POSITION);
            }
        }

        long sessionId() {
            return sessionId;
        }

        long lowWaterMark() {
            return lowWaterMark;
        }

        long localLowWaterMark() {
            return localLowWaterMark;
        }

        int flags() {
            return flags;
        }

        int sequenceNumber() {
            return structSequenceNumber;
        }

        void update(long sessionId, long lowWaterMark, long localLowWaterMark, int flags) {
            byteBuffer.putLong(position + SESSION_ID_POSITION, sessionId);
            byteBuffer.putLong(position + LOW_WATER_MARK_POSITION, lowWaterMark);
            byteBuffer.putLong(position + LOCAL_LOW_WATER_MARK_POSITION, localLowWaterMark);
            byteBuffer.putInt(position + FLAGS_POSITION, flags);
            byteBuffer.putInt(position + SEQUENCE_NUMBER_POSITION, sequenceNumber.incrementAndGet());
            byteBuffer.putInt(position + CHECKSUM_POSITION, Utils.checksum(byteBuffer, position, CHECKSUM_POSITION));

            this.sessionId = sessionId;
            this.lowWaterMark = lowWaterMark;
            this.localLowWaterMark = localLowWaterMark;
            this.flags = flags;
            this.structSequenceNumber = sequenceNumber.get();
        }

        void create() {
            if (sequenceNumber == null) {
                sequenceNumber = new AtomicInteger(-1);
            } else {
                sequenceNumber.incrementAndGet();
            }

            byteBuffer.putLong(position + SESSION_ID_POSITION, -1L);
            byteBuffer.putLong(position + LOW_WATER_MARK_POSITION, -1L);
            byteBuffer.putLong(position + LOCAL_LOW_WATER_MARK_POSITION, -1L);
            byteBuffer.putInt(position + FLAGS_POSITION, Flags.PARTITION_IS_AVAILABLE);
            byteBuffer.putInt(position + SEQUENCE_NUMBER_POSITION, sequenceNumber.get());
            byteBuffer.putInt(position + CHECKSUM_POSITION, Utils.checksum(byteBuffer, position, CHECKSUM_POSITION));

            this.sessionId = -1L;
            this.lowWaterMark = -1L;
            this.localLowWaterMark = -1L;
            this.flags = Flags.PARTITION_IS_AVAILABLE;
            this.structSequenceNumber = sequenceNumber.get();
        }

        boolean isValid() {
            return Utils.checksum(byteBuffer, position, CHECKSUM_POSITION) == byteBuffer.getInt(position + CHECKSUM_POSITION);
        }

        // for testing
        void invalidate() {
            int checksum = byteBuffer.getInt(position + CHECKSUM_POSITION);
            byteBuffer.putInt(position + CHECKSUM_POSITION, ~checksum);
        }

        public String toString() {
            return "sessionId=" + sessionId + " lowWaterMark=" + lowWaterMark + " localLowWaterMark=" + localLowWaterMark + " flags=" + flags + " sequenceNumber=" + structSequenceNumber;
        }
    }

    public static final class Flags {

        private Flags() {
        }

        public static final int PARTITION_IS_AVAILABLE = 1;
        public static final int PARTITION_IS_ASSIGNED = 4;

        public static boolean isFlagSet(int flags, int flag) {
            return (flags & flag) == flag;
        }
    }
}
