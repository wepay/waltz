package com.wepay.waltz.store.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.store.exception.GenerationMismatchException;
import com.wepay.waltz.store.exception.SessionClosedException;

import java.util.ArrayList;

public interface StoreSession {

    /**
     * Closes this store session
     */
    void close();

    /**
     * Returns the generation number of this session
     * @return the generation number
     */
    int generation();

    /**
     * Returns the high-water mark
     * @return the high-water mark
     */
    long highWaterMark();

    /**
     * Returns the low-water mark of this session
     * @return the low-water mark
     */
    long lowWaterMark();

    /**
     * Returns true if this session is writable
     * @return true if this session is writable, otherwise false
     */
    boolean isWritable();

    /**
     * Appends a transaction asynchronously.
     *
     * @param request the append request
     * @throws SessionClosedException
     * @throws GenerationMismatchException
     */
    void append(StoreAppendRequest request) throws SessionClosedException, GenerationMismatchException;

    /**
     * Returns the number of pending append requests
     * @return the number of pending append requests
     */
    int numPendingAppends();

    /**
     * Resolves all pending appending requests, which may or may not committed, using the given high-water mark.
     * The append operation is asynchronous. When it failed we do not know which transactions are committed until
     * a new session resolves the high-water mark. The new session should call this method of the failed session with
     * the resolved high-water mark.
     *
     * @param highWaterMark the high-water mark
     */
    void resolveAllAppendRequests(long highWaterMark);

    /**
     * Waits until all pending append request are processed and returns the high-water mark
     * @return high-water mark
     * @throws SessionClosedException
     */
    long flush() throws SessionClosedException;

    /**
     * Returns the record of the specified transaction
     * @param transactionId the transaction id
     * @return the transaction record
     * @throws SessionClosedException
     */
    Record getRecord(long transactionId) throws SessionClosedException;

    /**
     * Returns a list of records starting from the specified transaction
     * @param transactionId the transaction id
     * @return the transaction record
     * @throws SessionClosedException
     */
    ArrayList<Record> getRecordList(long transactionId, int maxNumRecords) throws SessionClosedException;

    /**
     * Returns the record header of the specified transaction
     * @param transactionId the transaction id
     * @return the transaction record header
     * @throws SessionClosedException
     */
    RecordHeader getRecordHeader(long transactionId) throws SessionClosedException;

    /**
     * Returns a list of record headers from the specified transaction
     * @param transactionId the transaction id
     * @return the transaction record header
     * @throws SessionClosedException
     */
    ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws SessionClosedException;

}
