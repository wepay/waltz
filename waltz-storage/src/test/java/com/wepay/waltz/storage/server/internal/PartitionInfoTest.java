package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.storage.exception.StorageException;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionInfoTest {
    @Test
    public void test() throws Exception {
        Path dir = Files.createTempDirectory("control-file-test");
        Path path = dir.resolve(ControlFile.FILE_NAME);
        int defaultFlags = PartitionInfo.Flags.PARTITION_IS_AVAILABLE;

        try {
            Random rand = new Random();
            UUID key = UUID.randomUUID();
            int numPartitions = 5;

            ControlFile controlFile = new ControlFile(key, path, numPartitions, true);

            PartitionInfo partitionInfo;

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);

                assertEquals(i, partitionInfo.partitionId);
                assertEquals(-1L, partitionInfo.sessionId());
                assertEquals(-1L, partitionInfo.getLowWaterMark());
                assertEquals(-1L, partitionInfo.getLocalLowWaterMark());
                assertEquals(defaultFlags, partitionInfo.getFlags());
                assertEquals(0, partitionInfo.getSequenceNumber());
            }

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);
                partitionInfo.setLowWaterMark(10 + i, 100 + i, 90 + i);

                assertEquals(i, partitionInfo.partitionId);
                assertEquals(10 + i, partitionInfo.sessionId());
                assertEquals(100 + i, partitionInfo.getLowWaterMark());
                assertEquals(90 + i, partitionInfo.getLocalLowWaterMark());
                assertEquals(defaultFlags, partitionInfo.getFlags());
                assertEquals(1, partitionInfo.getSequenceNumber());
            }

            controlFile.flush();
            controlFile.close();

            controlFile = new ControlFile(key, path, numPartitions, true);

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);

                assertEquals(i, partitionInfo.partitionId);
                assertEquals(10 + i, partitionInfo.sessionId());
                assertEquals(100 + i, partitionInfo.getLowWaterMark());
                assertEquals(90 + i, partitionInfo.getLocalLowWaterMark());
                assertEquals(defaultFlags, partitionInfo.getFlags());
                assertEquals(1, partitionInfo.getSequenceNumber());
            }

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);
                partitionInfo.setLowWaterMark(20 + i, 200 + i, 100 + i);

                assertEquals(i, partitionInfo.partitionId);
                assertEquals(20 + i, partitionInfo.sessionId());
                assertEquals(200 + i, partitionInfo.getLowWaterMark());
                assertEquals(100 + i, partitionInfo.getLocalLowWaterMark());
                assertEquals(defaultFlags, partitionInfo.getFlags());
                assertEquals(2, partitionInfo.getSequenceNumber());
            }

            int n = rand.nextInt(numPartitions);
            controlFile.getPartitionInfo(n).invalidate();

            controlFile.flush();
            controlFile.close();

            controlFile = new ControlFile(key, path, numPartitions, true);

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);

                if (i == n) {
                    assertEquals(10 + i, partitionInfo.sessionId());
                    assertEquals(100 + i, partitionInfo.getLowWaterMark());
                    assertEquals(90 + i, partitionInfo.getLocalLowWaterMark());
                    assertEquals(defaultFlags, partitionInfo.getFlags());
                    assertEquals(1, partitionInfo.getSequenceNumber());
                } else {
                    assertEquals(20 + i, partitionInfo.sessionId());
                    assertEquals(200 + i, partitionInfo.getLowWaterMark());
                    assertEquals(100 + i, partitionInfo.getLocalLowWaterMark());
                    assertEquals(defaultFlags, partitionInfo.getFlags());
                    assertEquals(2, partitionInfo.getSequenceNumber());
                }
            }

            controlFile.close();

        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                // ignore
            }
            try {
                Files.deleteIfExists(dir);
            } catch (IOException ex) {
                // ignore
            }
        }
    }


    @Test
    public void testFlags() throws Exception {
        Path dir = Files.createTempDirectory("control-file-test");
        Path path = dir.resolve(ControlFile.FILE_NAME);

        try {
            UUID key = UUID.randomUUID();
            int numPartitions = 5;

            ControlFile controlFile = new ControlFile(key, path, numPartitions, true);

            PartitionInfo partitionInfo;
            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);
                partitionInfo.setFlags(PartitionInfo.Flags.PARTITION_IS_AVAILABLE);

                assertEquals(-1, partitionInfo.sessionId());
                assertEquals(-1, partitionInfo.getLowWaterMark());
                assertEquals(-1, partitionInfo.getLocalLowWaterMark());
                assertEquals(PartitionInfo.Flags.PARTITION_IS_AVAILABLE, partitionInfo.getFlags());
                assertEquals(1, partitionInfo.getSequenceNumber());
            }

            controlFile.flush();
            controlFile.close();

            controlFile = new ControlFile(key, path, numPartitions, true);

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);

                assertEquals(-1, partitionInfo.sessionId());
                assertEquals(-1, partitionInfo.getLowWaterMark());
                assertEquals(-1, partitionInfo.getLocalLowWaterMark());
                assertEquals(PartitionInfo.Flags.PARTITION_IS_AVAILABLE, partitionInfo.getFlags());
                assertEquals(1, partitionInfo.getSequenceNumber());
            }

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);
                partitionInfo.setFlag(PartitionInfo.Flags.PARTITION_IS_ASSIGNED, true);
                partitionInfo.setFlag(PartitionInfo.Flags.PARTITION_IS_AVAILABLE, false);

                assertEquals(-1, partitionInfo.sessionId());
                assertEquals(-1, partitionInfo.getLowWaterMark());
                assertEquals(-1, partitionInfo.getLocalLowWaterMark());
                assertEquals(PartitionInfo.Flags.PARTITION_IS_ASSIGNED, partitionInfo.getFlags());
                assertEquals(3, partitionInfo.getSequenceNumber());
            }

            controlFile.flush();
            controlFile.close();

            controlFile = new ControlFile(key, path, numPartitions, true);

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);

                assertEquals(-1, partitionInfo.sessionId());
                assertEquals(-1, partitionInfo.getLowWaterMark());
                assertEquals(-1, partitionInfo.getLocalLowWaterMark());
                assertEquals(PartitionInfo.Flags.PARTITION_IS_ASSIGNED, partitionInfo.getFlags());
                assertEquals(3, partitionInfo.getSequenceNumber());
            }
        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                // ignore
            }
            try {
                Files.deleteIfExists(dir);
            } catch (IOException ex) {
                // ignore
            }
        }
    }

    @Test
    public void testSetLowWatermarkWithSameSessionId() throws Exception {
        Path dir = Files.createTempDirectory("control-file-test");
        Path path = dir.resolve(ControlFile.FILE_NAME);

        try {
            UUID key = UUID.randomUUID();
            ControlFile controlFile = new ControlFile(key, path, 1, true);

            PartitionInfo partitionInfo = controlFile.getPartitionInfo(0);

            // Verify new partition info is clean.
            assertEquals(0, partitionInfo.partitionId);
            assertEquals(-1L, partitionInfo.sessionId());
            assertEquals(-1L, partitionInfo.getLowWaterMark());
            assertEquals(-1L, partitionInfo.getLocalLowWaterMark());

            // Set the low watermark twice with the same session id.
            for (int i = 0; i < 2; ++i) {
                partitionInfo.setLowWaterMark(99, i, i + 1);

                assertEquals(0, partitionInfo.partitionId);
                assertEquals(99, partitionInfo.sessionId());
                assertEquals(i, partitionInfo.getLowWaterMark());
                assertEquals(i + 1, partitionInfo.getLocalLowWaterMark());
            }

            controlFile.flush();
            controlFile.close();

            // Re-open.
            controlFile = new ControlFile(key, path, 1, true);
            partitionInfo = controlFile.getPartitionInfo(0);

            // Try to set one more time with the same session id.
            partitionInfo.setLowWaterMark(99, 123, 456);

            assertEquals(0, partitionInfo.partitionId);
            assertEquals(99, partitionInfo.sessionId());
            assertEquals(123, partitionInfo.getLowWaterMark());
            assertEquals(456, partitionInfo.getLocalLowWaterMark());

            controlFile.close();

        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                // ignore
            }
            try {
                Files.deleteIfExists(dir);
            } catch (IOException ex) {
                // ignore
            }
        }
    }

    @Test
    public void testSetLowWatermarkWithBadWatermarks() throws Exception {
        Path dir = Files.createTempDirectory("control-file-test");
        Path path = dir.resolve(ControlFile.FILE_NAME);

        try {
            UUID key = UUID.randomUUID();
            ControlFile controlFile = new ControlFile(key, path, 1, true);

            PartitionInfo partitionInfo = controlFile.getPartitionInfo(0);

            // Verify new partition info is clean.
            assertEquals(0, partitionInfo.partitionId);
            assertEquals(-1L, partitionInfo.sessionId());
            assertEquals(-1L, partitionInfo.getLowWaterMark());
            assertEquals(-1L, partitionInfo.getLocalLowWaterMark());

            // Set initial values.
            partitionInfo.setLowWaterMark(99, 123, 456);

            assertEquals(0, partitionInfo.partitionId);
            assertEquals(99, partitionInfo.sessionId());
            assertEquals(123, partitionInfo.getLowWaterMark());
            assertEquals(456, partitionInfo.getLocalLowWaterMark());

            // Write smaller lowWaterMark.
            try {
                partitionInfo.setLowWaterMark(99, 122, 457);
                fail("expected to throw a storage exception above");
            } catch (StorageException e) {
                assertTrue(e.getMessage().contains("unable to set low watermark"));
            }

            // Write smaller localLowWatermark.
            try {
                partitionInfo.setLowWaterMark(99, 123, 455);
                fail("expected to throw a storage exception above");
            } catch (StorageException e) {
                assertTrue(e.getMessage().contains("unable to set local low watermark"));
            }

            partitionInfo.setLowWaterMark(99, 124, 457);

            // Verify writes work when both low watermarks are greater than previous.
            assertEquals(0, partitionInfo.partitionId);
            assertEquals(99, partitionInfo.sessionId());
            assertEquals(124, partitionInfo.getLowWaterMark());
            assertEquals(457, partitionInfo.getLocalLowWaterMark());

            controlFile.close();

        } finally {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                // ignore
            }
            try {
                Files.deleteIfExists(dir);
            } catch (IOException ex) {
                // ignore
            }
        }
    }
}
