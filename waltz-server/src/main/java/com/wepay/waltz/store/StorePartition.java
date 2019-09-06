package com.wepay.waltz.store;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.store.exception.StoreException;

import java.util.ArrayList;
import java.util.function.LongConsumer;

/**
 * This class handles the store partition.
 */
public interface StorePartition {

    /**
     * Closes the store partition.
     */
    void close();

    /**
     * Returns the partition Id.
     * @return the partition Id.
     */
    int partitionId();

    /**
     * Sets the generation number.
     * @param generation The generation number.
     */
    void generation(int generation);

    /**
     * Returns the generation number.
     * @return the generation number.
     */
    int generation();

    /**
     * Checks if the store partition is healthy.
     * @return True if healthy, otherwise return False.
     */
    boolean isHealthy();

    /**
     * Appends the request to the store partition.
     * @param reqId The request Id.
     * @param header The request header.
     * @param data The transaction data.
     * @param checksum The checksum.
     * @param onCompletion Represents what to do on completion.
     * @throws StoreException thrown if failed to access the store.
     */
    void append(ReqId reqId, int header, byte[] data, int checksum, LongConsumer onCompletion) throws StoreException;

    /**
     * Returns the record header of the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link RecordHeader}.
     * @throws StoreException thrown if failed to access the store.
     */
    RecordHeader getRecordHeader(long transactionId) throws StoreException;

    /**
     * Returns {@link RecordHeader}s for the given list of transaction Ids.
     * @param transactionId The transaction Id.
     * @param maxNumRecords The maximum number of records.
     * @return list of {@link RecordHeader}s.
     * @throws StoreException thrown if failed to access the store.
     */
    ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StoreException;

    /**
     * Returns {@link Record} for the given transaction Id.
     * @param transactionId The transaction Id.
     * @return the {@link Record}.
     * @throws StoreException thrown if failed to access the store.
     */
    Record getRecord(long transactionId) throws StoreException;

    /**
     * Returns the high-water mark of this store partition.
     * @return the high-water mark.
     * @throws StoreException thrown if failed to access the store.
     */
    long highWaterMark() throws StoreException;

    /**
     * Returns the number of pending append requests.
     * @return the number of pending append requests.
     */
    int numPendingAppends();

    /**
     * Flushes the store partition.
     * @return the high-water mark of the store partition.
     * @throws StoreException thrown if failed to access the store.
     */
    long flush() throws StoreException;

}
