package com.wepay.waltz.storage.server.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.storage.exception.StorageException;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A storage node's control file contains metadata information. The metadata includes the data in
 * {@link com.wepay.waltz.storage.server.internal.ControlFileHeader} as well as a {@link PartitionInfo} array that
 * has an entry for each partition in the cluster.
 */
public class ControlFile {

    private static final Logger logger = Logging.getLogger(ControlFile.class);

    public static final String FILE_NAME = "waltz-storage.ctl";

    private static final int HEADER_SIZE = 128;

    public final UUID key;

    private final FileLock lock;
    private final FileChannel channel;
    private final PartitionInfo[] infos;

    /**
     * Constructing a control file object opens up a file channel on disk to the control file. If the control file is
     * being constructed for the first time, the control file is initialized with a {@link ControlFileHeader}. Once the
     * control file is created or re-opened, {@link PartitionInfo} objects are created for each partition.
     *
     * @param key universally unique identifier for cluster
     * @param controlFile path to control file
     * @param numPartitions number of partitions in the cluster
     * @param check whether or not to verify that the provided key and numPartitions match what's already in a control
     *              file.. When key is not null, check should always be true; when called by
     *              {@link com.wepay.waltz.common.util.Cli} class, check can be false.
     * @throws StorageException
     * @throws IOException
     */
    public ControlFile(UUID key, Path controlFile, int numPartitions, boolean check) throws StorageException, IOException {
        if (!check && key != null) {
            throw new StorageException("key should not be provided when check is false");
        }

        // Open the control file and acquire the lock.
        channel = FileChannel.open(controlFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        lock = channel.tryLock(0, HEADER_SIZE, false);

        if (lock == null) {
            logger.error("failed to acquire the lock");
            throw new StorageException("failed to acquire the lock");
        }

        try {
            final boolean initial = channel.size() < HEADER_SIZE;
            ControlFileHeader header;

            if (initial) {
                // This is the first time. Initialize the header.
                logger.debug("initializing control file: {}", controlFile);

                ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
                header = new ControlFileHeader(ControlFileHeader.VERSION, System.currentTimeMillis(), key, numPartitions);
                header.writeTo(headerBuffer);
                headerBuffer.flip();
                channel.position(0);
                while (headerBuffer.remaining() > 0) {
                    channel.write(headerBuffer);
                }
            } else {
                // Read the header and verify the key and the number of partitions.
                ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
                while (headerBuffer.remaining() > 0) {
                    channel.read(headerBuffer);
                }
                headerBuffer.flip();
                header = ControlFileHeader.readFrom(headerBuffer);

                if (check && !header.key.equals(key)) {
                    throw new StorageException("the cluster key does not match");
                }
                if (check && header.numPartitions != numPartitions) {
                    throw new StorageException("the number of partitions does not match");
                }
            }
            this.key = header.key;

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, HEADER_SIZE, (long) PartitionInfo.SIZE * (long) header.numPartitions);
            infos = new PartitionInfo[header.numPartitions];

            int position = 0;
            for (int partitionId = 0; partitionId < header.numPartitions; partitionId++) {
                infos[partitionId] = new PartitionInfo(buffer, position, channel, partitionId, initial);

                if (infos[partitionId].partitionId != partitionId) {
                    throw new StorageException("the partition id does not match");
                }

                position += PartitionInfo.SIZE;
            }
            channel.force(true);

        } catch (StorageException | IOException ex) {
            try {
                lock.release();
            } catch (IOException ignore) {
            }
            try {
                channel.close();
            } catch (IOException ignore) {
            }
            throw ex;
        }
    }

    /**
     * Return a cached {@link PartitionInfo} object from the control file's in-memory {@link PartitionInfo} map cache.
     *
     * @param partitionId
     * @return A PartitionInfo object
     */
    public PartitionInfo getPartitionInfo(int partitionId) {
        return infos[partitionId];
    }

    /**
     * Return the corresponding {@link PartitionInfoSnapshot} object for the cached {@link PartitionInfo}
     * from the control file's in-memory {@link PartitionInfo} map cache.
     *
     * @param partitionId
     * @return A PartitionInfoSnapshot object
     */
    public PartitionInfoSnapshot getPartitionInfoSnapshot(int partitionId) {
        return getPartitionInfo(partitionId).getSnapshot();
    }

    /**
     * Flush the file channel for the control file.
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        channel.force(false);
    }

    /**
     * Close the connection to the control file's file channel. This method does not affect the {@link PartitionInfo}
     * cache.
     */
    public void close() {
        try {
            lock.release();
        } catch (IOException ex) {
            logger.error("failed to unlock the control file", ex);
        }

        try {
            channel.force(true);
            channel.close();
        } catch (IOException ex) {
            logger.error("failed to close the control file", ex);
        }
    }

    /**
     * Fetch all partition IDs for this control file.
     * @return A set of partition IDs.
     */
    public Set<Integer> getPartitionIds() {
        return Arrays.stream(infos).map(x -> x.partitionId).collect(Collectors.toSet());
    }

    /**
     * Returns the number of partitions in this control file.
     *
     * @return Number of partitions.
     */
    public int getNumPartitions() {
        return infos.length;
    }
}
