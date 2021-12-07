package com.wepay.waltz.tools;

import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

public final class CliUtils {

    private CliUtils() { }

    /**
     * A transaction callback to help construct {@link WaltzClient}. It is dummy because
     * it is not suppose to receive any callbacks.
     */
    public static final class DummyTxnCallbacks implements WaltzClientCallbacks {

        @Override
        public long getClientHighWaterMark(int partitionId) {
            return -1L;
        }

        @Override
        public void applyTransaction(Transaction transaction) {
        }

        @Override
        public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
        }
    }

    /**
     * Given a String containing comma-separated integer ranges, expands those ranges, de-duplicates,
     * and returns as a List.
     * For "0-2,3,4-5", returns [0,1,2,3,4,5]
     * @param rangesString Integer ranges as a String
     * @return expanded ranges as a {@link List}
     */
    public static List<Integer> parseIntRanges(String rangesString) {
        if (rangesString == null || rangesString.isEmpty()) {
            throw new IllegalArgumentException("Invalid input " + rangesString);
        }

        Set<Integer> partitions = new HashSet<>();
        String[] ranges = rangesString.split(",");

        for (String range : ranges) {
            if (range.contains("-")) {
                int min = Integer.parseInt(range.split("-")[0].trim());
                int max = Integer.parseInt(range.split("-")[1].trim());
                IntStream.rangeClosed(min, max).forEach(partitions::add);
            } else {
                partitions.add(Integer.parseInt(range.trim()));
            }
        }

        return new ArrayList<>(partitions);
    }
}
