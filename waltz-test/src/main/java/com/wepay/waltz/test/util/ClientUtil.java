package com.wepay.waltz.test.util;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.common.util.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;

public final class ClientUtil {
    private static final Random RANDOM = new Random();

    private ClientUtil() {
    }

    /**
     * Makes records with transaction ids between {@code startTransactionId} (inclusive) and
     * {@code endTransactionId} (exclusive).
     * @param startTransactionId
     * @param endTransactionId
     * @return
     */
    public static ArrayList<Record> makeRecords(long startTransactionId, long endTransactionId) {
        ArrayList<Record> records = new ArrayList<>();
        long transactionId = startTransactionId;
        while (transactionId < endTransactionId) {
            byte[] data = generateData(transactionId);
            Record record = new Record(transactionId++, reqId(), 0, data, Utils.checksum(data));
            records.add(record);
        }
        return records;
    }

    /**
     * Generates transaction data for the transaction id deterministically
     * @param transactionId
     * @return transaction data
     */
    public static byte[] generateData(long transactionId) {
        return Long.toOctalString(transactionId).getBytes(StandardCharsets.UTF_8);
    }

    private static ReqId reqId() {
        return new ReqId(RANDOM.nextLong(), RANDOM.nextLong());
    }
}
