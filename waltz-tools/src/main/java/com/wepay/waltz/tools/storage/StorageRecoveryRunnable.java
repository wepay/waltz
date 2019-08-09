package com.wepay.waltz.tools.storage;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.storage.common.SessionInfo;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.internal.ReplicaConnection;
import com.wepay.waltz.store.internal.ReplicaConnectionImpl;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * Copies records from a partition on one storage node to the same partition on another storage node. Stops when the
 * destination storage node has all records from the source up to the source node's low watermark. Note that the
 * recovery does not refresh the source storage node's low watermark once it starts running, so if the recovery takes
 * a long period of time, there might still be a substantial gap between the source node's current low watermark, and
 * the destination's.
 */
public class StorageRecoveryRunnable implements Runnable {

    private static final Logger logger = Logging.getLogger(StorageRecoveryRunnable.class);

    private static final int MAX_BATCH_SIZE = 20;
    private static final int LOG_SIZE = 10000;

    private final StorageAdminClient sourceStorageAdminClient;
    private final StorageAdminClient destinationStorageAdminClient;
    private final StorageClient destinationStorageClient;
    private final int partitionId;
    private final int maxBatchSize;

    /**
     * @param sourceStorageAdminClient The admin client to read records from source storage
     * @param destinationStorageAdminClient The admin client to read {@code SessionInfo} from destination storage
     * @param destinationStorageClient The storage client used to build {@code destinationReplicaConnection}
     * @param destinationStorageAdminClient The storage node to write records to
     */
    public StorageRecoveryRunnable(StorageAdminClient sourceStorageAdminClient, StorageAdminClient destinationStorageAdminClient, StorageClient destinationStorageClient, int partitionId) {
        this(sourceStorageAdminClient, destinationStorageAdminClient, destinationStorageClient, partitionId, MAX_BATCH_SIZE);
    }

    /**
     * @param sourceStorageAdminClient The admin client to read records from source storage
     * @param destinationStorageAdminClient The admin client to read {@code SessionInfo} from destination storage
     * @param destinationStorageClient The storage client used to build {@code destinationReplicaConnection}
     * @param destinationStorageAdminClient The storage node to write records to
     * @param maxBatchSize The largest number of records to fetch from the source node in one request
     */
    public StorageRecoveryRunnable(StorageAdminClient sourceStorageAdminClient, StorageAdminClient destinationStorageAdminClient, StorageClient destinationStorageClient, int partitionId, int maxBatchSize) {
        this.sourceStorageAdminClient = sourceStorageAdminClient;
        this.destinationStorageAdminClient = destinationStorageAdminClient;
        this.destinationStorageClient = destinationStorageClient;
        this.partitionId = partitionId;
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * Run the recovery. Loading is done in batches of {@code maxBatchSize}. The low watermark to catch up to is
     * determined at the start of the run method and is not refreshed after the initial resolution. If recovery takes a
     * long period of time, the destination node might still lag significantly behind the source node after recovery
     * completes. If this is undesirable, run can be called multiple times to do sequential recoveries.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        ReplicaConnection destinationReplicaConnection = null;
        try {
            SessionInfo sessionInfo = (SessionInfo) destinationStorageAdminClient.lastSessionInfo(partitionId).get();
            long destinationSessionId = sessionInfo.sessionId;
            long destinationLowWatermark = sessionInfo.lowWaterMark;

            // Create destination storage connection
            destinationReplicaConnection = new ReplicaConnectionImpl(partitionId, destinationSessionId, destinationStorageClient);

            // Get destination storage current high watermark
            long currentHighWaterMark = destinationReplicaConnection.getMaxTransactionId();
            logger.debug("destination high watermark = " + currentHighWaterMark);

            // Truncate destination storage to its low watermark
            currentHighWaterMark = destinationReplicaConnection.truncate(destinationLowWatermark);
            logger.info("destination truncated to low watermark = " + destinationLowWatermark);

            // Get stopping point from source node
            SessionInfo sourceSessionInfo = (SessionInfo) sourceStorageAdminClient.lastSessionInfo(destinationReplicaConnection.getPartitionId()).get();
            long targetLowWaterMark = sourceSessionInfo.localLowWaterMark;
            logger.info("source target low watermark = " + targetLowWaterMark);

            while (currentHighWaterMark < targetLowWaterMark) {
                int batchSize = (int) Math.min((targetLowWaterMark - currentHighWaterMark), (long) maxBatchSize);
                ArrayList<Record> records = (ArrayList<Record>) sourceStorageAdminClient
                        .getRecordList(destinationReplicaConnection.getPartitionId(), currentHighWaterMark + 1, batchSize)
                        .get();
                destinationReplicaConnection.appendRecords(records);
                currentHighWaterMark += records.size();
                destinationReplicaConnection.setLowWaterMark(currentHighWaterMark);
                logger.debug("destination high watermark = " + currentHighWaterMark);

                if (currentHighWaterMark % LOG_SIZE == 0) {
                    float percentProgress = currentHighWaterMark / (float) targetLowWaterMark;
                    logger.info(String.format("destination high watermark = %d (%.2f%%)", currentHighWaterMark, percentProgress));
                }
            }
        } catch (StorageRpcException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            if (destinationReplicaConnection != null) {
                destinationReplicaConnection.close();
            }
        }
    }
}
