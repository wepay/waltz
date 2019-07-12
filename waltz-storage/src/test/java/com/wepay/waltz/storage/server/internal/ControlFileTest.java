package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.storage.exception.StorageException;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ControlFileTest {

    @Test
    public void test() throws Exception {
        Path dir = Files.createTempDirectory("control-file-test");
        Path path = dir.resolve(ControlFile.FILE_NAME);
        int defaultFlags = PartitionInfo.Flags.PARTITION_IS_AVAILABLE;

        try {
            UUID key = UUID.randomUUID();
            int numPartitions = 3;

            ControlFile controlFile = new ControlFile(key, path, numPartitions, true);

            PartitionInfo partitionInfo;

            for (int i = 0; i < numPartitions; i++) {
                partitionInfo = controlFile.getPartitionInfo(i);

                assertEquals(i, partitionInfo.partitionId);
                assertEquals(-1L, partitionInfo.sessionId());
                assertEquals(-1L, partitionInfo.getLowWaterMark());
                assertEquals(defaultFlags, partitionInfo.getFlags());
                assertEquals(0, partitionInfo.getSequenceNumber());
            }

            controlFile.flush();
            controlFile.close();

            try {
                controlFile = new ControlFile(UUID.randomUUID(), path, numPartitions, true);
                controlFile.close();
                fail();

            } catch (StorageException ex) {
                //
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

}
