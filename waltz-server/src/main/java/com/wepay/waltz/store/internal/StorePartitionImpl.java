package com.wepay.waltz.store.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.BackoffTimer;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.StorePartition;
import com.wepay.waltz.store.exception.RecoveryFailedException;
import com.wepay.waltz.store.exception.SessionClosedException;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.store.exception.StorePartitionClosedException;
import com.wepay.waltz.store.exception.StoreSessionManagerException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.function.LongConsumer;

public class StorePartitionImpl implements StorePartition {

    private static final Logger logger = Logging.getLogger(StorePartitionImpl.class);

    private final long initialRetryInterval;
    private final BackoffTimer backoffTimer;
    private final int checkpointInterval;
    private final StoreSessionManager storeSessionManager;

    private volatile boolean running = true;

    public StorePartitionImpl(StoreSessionManager storeSessionManager, WaltzServerConfig config) {
        this.initialRetryInterval = (long) config.get(WaltzServerConfig.INITIAL_RETRY_INTERVAL);
        this.backoffTimer = new BackoffTimer((long) config.get(WaltzServerConfig.MAX_RETRY_INTERVAL));
        this.checkpointInterval = (int) config.get(WaltzServerConfig.CHECKPOINT_INTERVAL);
        this.storeSessionManager = storeSessionManager;
    }

    public void close() {
        running = false;

        backoffTimer.close();
        storeSessionManager.close();
    }

    @Override
    public int partitionId() {
        return storeSessionManager.partitionId;
    }

    @Override
    public void generation(int generation) {
        storeSessionManager.generation(generation);
    }

    @Override
    public int generation() {
        return storeSessionManager.generation();
    }

    @Override
    public boolean isHealthy() {
        return storeSessionManager.isHealthy();
    }

    @Override
    public void append(ReqId reqId, int header, byte[] data, int checksum, LongConsumer onCompletion) throws StoreException {
        StoreAppendRequest request = new StoreAppendRequest(reqId, header, data, checksum, onCompletion);

        long retryInterval = initialRetryInterval;

        while (running) {
            try {
                StoreSession session = storeSessionManager.getStoreSession(reqId.generation());
                if (session.highWaterMark() - session.lowWaterMark() >= checkpointInterval) {
                    session.flush();
                    session.close();
                } else {
                    session.append(request);
                    return;
                }
            } catch (SessionClosedException ex) {
                // Retry
            } catch (StoreSessionManagerException ex) {
                throw new StorePartitionClosedException();
            } catch (RecoveryFailedException ex) {
                logger.warn("recovery failed, retrying...", ex);
            }
            retryInterval = backoffTimer.backoff(retryInterval);
        }
        // This store partition is closed
        throw new StorePartitionClosedException();
    }

    @Override
    public Record getRecord(long transactionId) throws StoreException {
        long retryInterval = initialRetryInterval;

        while (running) {
            try {
                StoreSession session = storeSessionManager.getStoreSession();
                return session.getRecord(transactionId);

            } catch (SessionClosedException ex) {
                // Retry
            } catch (StoreSessionManagerException ex) {
                throw new StorePartitionClosedException();
            } catch (RecoveryFailedException ex) {
                logger.warn("recovery failed, retrying...", ex);
            }
            retryInterval = backoffTimer.backoff(retryInterval);
        }
        throw new StorePartitionClosedException();
    }

    @Override
    public RecordHeader getRecordHeader(long transactionId) throws StoreException {
        long retryInterval = initialRetryInterval;

        while (running) {
            try {
                StoreSession session = storeSessionManager.getStoreSession();
                return session.getRecordHeader(transactionId);

            } catch (SessionClosedException ex) {
                // Retry
            } catch (StoreSessionManagerException ex) {
                throw new StorePartitionClosedException();
            } catch (RecoveryFailedException ex) {
                logger.warn("recovery failed, retrying...", ex);
            }
            retryInterval = backoffTimer.backoff(retryInterval);
        }
        throw new StorePartitionClosedException();
    }

    @Override
    public ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StoreException {
        long retryInterval = initialRetryInterval;

        while (running) {
            try {
                StoreSession session = storeSessionManager.getStoreSession();
                return session.getRecordHeaderList(transactionId, maxNumRecords);

            } catch (SessionClosedException ex) {
                // Retry
            } catch (StoreSessionManagerException ex) {
                throw new StorePartitionClosedException();
            } catch (RecoveryFailedException ex) {
                logger.warn("recovery failed, retrying...", ex);
            }
            retryInterval = backoffTimer.backoff(retryInterval);
        }
        throw new StorePartitionClosedException();
    }

    @Override
    public long highWaterMark() throws StoreException {
        // Get the high-water mark from the store
        long retryInterval = initialRetryInterval;

        while (running) {
            try {
                return storeSessionManager.getStoreSession().highWaterMark();

            } catch (StoreSessionManagerException ex) {
                throw new StorePartitionClosedException();
            } catch (RecoveryFailedException ex) {
                logger.warn("recovery failed, retrying...", ex);
            }
            retryInterval = backoffTimer.backoff(retryInterval);
        }

        throw new StorePartitionClosedException();
    }

    @Override
    public int numPendingAppends() {
        return storeSessionManager.numPendingAppends();
    }

    @Override
    public long flush() throws StoreException {
        long retryInterval = initialRetryInterval;

        while (running) {
            try {
                StoreSession session = storeSessionManager.getStoreSession();
                return session.flush();

            } catch (SessionClosedException ex) {
                // Retry
            } catch (StoreSessionManagerException ex) {
                throw new StorePartitionClosedException();
            } catch (RecoveryFailedException ex) {
                logger.warn("recovery failed, retrying...", ex);
            }
            retryInterval = backoffTimer.backoff(retryInterval);
        }
        throw new StorePartitionClosedException();
    }

}
