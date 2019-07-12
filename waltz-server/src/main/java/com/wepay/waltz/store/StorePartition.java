package com.wepay.waltz.store;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.RecordHeader;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.store.exception.StoreException;

import java.util.ArrayList;
import java.util.function.LongConsumer;

public interface StorePartition {

    void close();

    int partitionId();

    void generation(int generation);

    int generation();

    boolean isHealthy();

    void append(ReqId reqId, int header, byte[] data, int checksum, LongConsumer onCompletion) throws StoreException;

    RecordHeader getRecordHeader(long transactionId) throws StoreException;

    ArrayList<RecordHeader> getRecordHeaderList(long transactionId, int maxNumRecords) throws StoreException;

    Record getRecord(long transactionId) throws StoreException;

    long highWaterMark() throws StoreException;

    int numPendingAppends();

    long flush() throws StoreException;

}
