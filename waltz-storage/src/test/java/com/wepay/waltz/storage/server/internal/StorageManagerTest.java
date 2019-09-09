package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.storage.exception.ConcurrentUpdateException;
import com.wepay.waltz.storage.exception.StorageException;
import com.wepay.waltz.storage.WaltzStorageConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageManagerTest {
    private static final int NUM_PARTITION = 3;
    private static final UUID clusterKey = UUID.randomUUID();

    @Test
    public void testGetPartitionInfos() throws IOException, StorageException {
        File dir = Files.createTempDirectory("StorageManagerTest-").toFile();
        StorageManager manager = new StorageManager(dir.getPath(), WaltzStorageConfig.DEFAULT_SEGMENT_SIZE_THRESHOLD, NUM_PARTITION, clusterKey, WaltzStorageConfig.DEFAULT_STORAGE_SEGMENT_CACHE_CAPACITY);

        try {
            manager.open(clusterKey, NUM_PARTITION);

            List<PartitionInfoSnapshot> partitionInfos = manager.getPartitionInfos()
                    .stream()
                    .sorted(Comparator.comparingInt(o -> o.partitionId))
                    .collect(Collectors.toList());

            assertEquals(NUM_PARTITION, partitionInfos.size());
            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfoSnapshot expectedPartitionInfoSnapshot = new PartitionInfoSnapshot(i, -1L, -1L, -1, PartitionInfo.Flags.PARTITION_IS_AVAILABLE);
                assertEquals(expectedPartitionInfoSnapshot, partitionInfos.get(i));
            }
        } finally {
            manager.close();
        }
    }

    @Test (expected = StorageException.class)
    public void testStorageOpenWithWrongKey() throws IOException, StorageException {
        File dir = Files.createTempDirectory("StorageManagerTest-").toFile();
        StorageManager manager = new StorageManager(dir.getPath(), WaltzStorageConfig.DEFAULT_SEGMENT_SIZE_THRESHOLD, NUM_PARTITION, clusterKey, WaltzStorageConfig.DEFAULT_STORAGE_SEGMENT_CACHE_CAPACITY);

        try {
            manager.open(UUID.randomUUID(), NUM_PARTITION);
        } finally {
            manager.close();
        }
    }

    @Test
    public void testRemovePartitionWithFilePreserve() throws ConcurrentUpdateException, IOException, StorageException {
        int partitionId = new Random().nextInt(NUM_PARTITION);
        String fileNameFormat = "%019d.%s";
        File dir = Files.createTempDirectory("StorageManagerTest-").toFile();
        Path partitionDir = FileSystems.getDefault().getPath(dir.getPath()).resolve(Integer.toString(partitionId));
        Path segPath = partitionDir.resolve(String.format(fileNameFormat, 0L, "seg"));
        Path idxPath = partitionDir.resolve(String.format(fileNameFormat, 0L, "idx"));

        StorageManager manager = new StorageManager(dir.getPath(), WaltzStorageConfig.DEFAULT_SEGMENT_SIZE_THRESHOLD, NUM_PARTITION, clusterKey, WaltzStorageConfig.DEFAULT_STORAGE_SEGMENT_CACHE_CAPACITY);

        try {
            manager.open(clusterKey, NUM_PARTITION);
            manager.setPartitionAssignment(partitionId, true, false);

            manager.setPartitionAssignment(partitionId, false, false);

            assertTrue(segPath.toFile().exists());
            assertTrue(idxPath.toFile().exists());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testRemovePartitionWithoutFilePreserve() throws ConcurrentUpdateException, IOException, StorageException {
        int partitionId = new Random().nextInt(NUM_PARTITION);
        String fileNameFormat = "%019d.%s";
        File dir = Files.createTempDirectory("StorageManagerTest-").toFile();
        Path partitionDir = FileSystems.getDefault().getPath(dir.getPath()).resolve(Integer.toString(partitionId));
        Path segPath = partitionDir.resolve(String.format(fileNameFormat, 0L, "seg"));
        Path idxPath = partitionDir.resolve(String.format(fileNameFormat, 0L, "idx"));

        StorageManager manager = new StorageManager(dir.getPath(), WaltzStorageConfig.DEFAULT_SEGMENT_SIZE_THRESHOLD, NUM_PARTITION, clusterKey, WaltzStorageConfig.DEFAULT_STORAGE_SEGMENT_CACHE_CAPACITY);

        try {
            manager.open(clusterKey, NUM_PARTITION);
            manager.setPartitionAssignment(partitionId, true, false);

            manager.setPartitionAssignment(partitionId, false, true);

            assertFalse(segPath.toFile().exists());
            assertFalse(idxPath.toFile().exists());
        } finally {
            manager.close();
        }
    }
}
