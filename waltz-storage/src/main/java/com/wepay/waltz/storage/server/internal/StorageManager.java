package com.wepay.waltz.storage.server.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.storage.exception.ConcurrentUpdateException;
import com.wepay.waltz.storage.exception.StorageException;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Coordinates {@link ControlFile} and {@link Partition} object creation and manages a storage node's directory
 * structure.
 */
public class StorageManager {

    private static final Logger logger = Logging.getLogger(StorageManager.class);

    private final Path directory;
    private final HashMap<Integer, Partition> partitions;
    private final long segmentSizeThreshold;
    private final int segmentCacheCapacity;

    private ControlFile controlFile = null;
    private boolean running = true;

    /**
     * This method initializes private data members of this class and also creates the Control File.
     *
     * @param directory The root directory of Storage where the transaction data is stored.
     * @param segmentSizeThreshold The maximum size of each Segment.
     * @param numPartitions The total number of partitions in the cluster.
     * @param key The cluster key.
     * @throws StorageException
     * @throws IOException
     */
    public StorageManager(String directory, long segmentSizeThreshold, int numPartitions, UUID key, int segmentCacheCapacity) throws IOException, StorageException {
        logger.debug("StorageManager constructor is called");
        this.directory = FileSystems.getDefault().getPath(directory);
        this.partitions = new HashMap<>();
        this.segmentSizeThreshold = segmentSizeThreshold;
        this.segmentCacheCapacity = segmentCacheCapacity;

        this.controlFile = new ControlFile(key, this.directory.resolve(ControlFile.FILE_NAME), numPartitions, true);
        logger.debug("storage opened: directory={}", directory);

        for (int id : controlFile.getPartitionIds()) {
            if (controlFile.getPartitionInfo(id).getSnapshot().isAssigned) {
                createPartition(id);
            }
        }
    }

    /**
     * Open a control file. This method caches the control file object it creates, so it only needs to be called once.
     *
     * @param key The cluster key.
     * @param numPartitions The total number of partitions in the cluster.
     * @throws StorageException
     * @throws IOException
     */
    public void open(UUID key, int numPartitions) throws StorageException {
        synchronized (this) {
            if (running) {
                logger.debug("StorageManager.open() is called");
                if (!key.equals(controlFile.key)) {
                    throw new StorageException("Provided cluster key is inconsistent with key in control file.");
                }
                if (numPartitions != controlFile.getNumPartitions()) {
                    throw new StorageException("Provided number of partitions is inconsistent with the total number of partitions in control file.");
                }
            } else {
                throw new StorageException("Control file is already closed.");
            }
        }
    }

    /**
     * Returns a {@link Partition} object. This method caches the partition object if it's already been fetched before.
     *
     * @param partitionId The id of the partition to fetch
     * @return The {@link Partition} object if its created or else it will return null.
     * @throws StorageException
     */
    Partition getPartition(int partitionId) throws StorageException {
        synchronized (this) {
            if (running) {
                return partitions.get(partitionId);
            } else {
                throw new StorageException("Control file is already closed.");
            }
        }
    }

    /**
     * This method creates a new partition object.
     *
     * @param partitionId The id of the partition to create
     * @throws StorageException
     * @throws IOException
     */
    private void createPartition(int partitionId) throws StorageException, IOException {
        synchronized (this) {
            if (running) {
                if (getPartition(partitionId) == null) {
                    PartitionInfo partitionInfo = controlFile.getPartitionInfo(partitionId);
                    Path partitionDir = directory.resolve(Integer.toString(partitionId));

                    if (!Files.exists(partitionDir)) {
                        Files.createDirectory(partitionDir);
                    }

                    Partition partition = new Partition(controlFile.key, partitionDir, partitionInfo, segmentSizeThreshold, segmentCacheCapacity);
                    partition.open();
                    partitions.put(partitionId, partition);
                } else {
                    logger.debug("Partition:" + partitionId + " creation is skipped as it already exists.");
                }
            } else {
                throw new StorageException("Control file is already closed.");
            }
        }
    }

    public void setPartitionAssignment(int partitionId, boolean isAssigned, boolean deleteStorageFiles) throws ConcurrentUpdateException, IOException, StorageException {
        synchronized (this) {
            if (running) {
                PartitionInfo partitionInfo = controlFile.getPartitionInfo(partitionId);
                if (isAssigned) {
                    createPartition(partitionId);
                    partitionInfo.setFlag(PartitionInfo.Flags.PARTITION_IS_ASSIGNED, true);
                } else {
                    partitionInfo.reset();

                    Partition partition = getPartition(partitionId);
                    partition.close();

                    if (deleteStorageFiles) {
                        partition.deleteSegments();
                    }

                    partitions.remove(partitionId);
                }
            } else {
                throw new StorageException("Control file is already closed.");
            }
        }
    }

    public void setPartitionAvailable(int partitionId, boolean isAvailable) throws ConcurrentUpdateException, IOException, StorageException {
        synchronized (this) {
            if (running) {
                PartitionInfo partitionInfo = controlFile.getPartitionInfo(partitionId);
                partitionInfo.setFlag(PartitionInfo.Flags.PARTITION_IS_AVAILABLE, isAvailable);
            } else {
                throw new StorageException("Control file is already closed.");
            }
        }
    }

    public void close() {
        synchronized (this) {
            running = false;

            for (Partition partition : partitions.values()) {
                partition.close();
            }
            partitions.clear();

            if (controlFile != null) {
                controlFile.close();
            }
        }
    }

    public int checksum(int partitionId) throws StorageException, IOException {
        Partition partition = getPartition(partitionId);
        return (partition != null) ? partition.checksum() : -1;
    }

    public Set<PartitionInfoSnapshot> getPartitionInfos() throws StorageException, IOException {
        synchronized (this) {
            if (running) {
                if (controlFile == null) {
                    throw new StorageException("Control file is not opened, cannot get partition info.");
                }

                Set<PartitionInfoSnapshot> infos = new HashSet<>();
                for (int id : controlFile.getPartitionIds()) {
                    infos.add(controlFile.getPartitionInfo(id).getSnapshot());
                }
                return infos;
            } else {
                throw new StorageException("Control file is already closed.");
            }
        }
    }

    public Set<Integer> getAssignedPartitionIds() throws StorageException, IOException {
        synchronized (this) {
            if (running) {
                if (controlFile == null) {
                    throw new StorageException("Control file is not opened, cannot get partition info.");
                }
                Set<Integer> partitionIds = new HashSet<>();
                for (int id : controlFile.getPartitionIds()) {
                    if (controlFile.getPartitionInfo(id).getSnapshot().isAssigned) {
                        partitionIds.add(id);
                    }
                }
                return partitionIds;
            } else {
                throw new StorageException("Control file is already closed.");
            }
        }
    }
}
