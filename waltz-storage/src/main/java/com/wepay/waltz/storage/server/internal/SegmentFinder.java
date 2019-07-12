package com.wepay.waltz.storage.server.internal;

import java.util.ArrayList;

public final class SegmentFinder {

    private SegmentFinder() {
        //not called
    }

    /**
     * Find segment that contains the transaction. Consider the transaction is
     * likely to store in recent segments, here we use exponential search.
     *
     * @param segments an list of segments
     * @param transactionId id for each transaction
     * @return segment that contains the transaction, or null if not found
     */
     static Segment findSegment(ArrayList<Segment> segments, long transactionId) {
        synchronized (segments) {
            int n = segments.size();
            Segment lastSegment = segments.get(n - 1);

            // If the transaction is in the last segment
            if (transactionId >= lastSegment.firstTransactionId()) {
                return transactionId <= lastSegment.maxTransactionId() ? lastSegment : null;
            }

            // Find range for binary search by repeated doubling
            int i = 1;
            while (i < n && segments.get(n - 1 - i).firstTransactionId() > transactionId) {
                i *= 2;
            }

            // Call binary search for the found range
            return binarySearch(segments, Math.max(0, n - 1 - i), n - 1 - (i / 2), transactionId);
        }
    }

    private static Segment binarySearch(ArrayList<Segment> segments, int low, int high, long transactionId) {
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Segment midSegment = segments.get(mid);
            if (midSegment.maxTransactionId() < transactionId) {
                low = mid + 1;
            } else if (midSegment.firstTransactionId() > transactionId) {
                high = mid - 1;
            } else {
                return midSegment;
            }
        }
        return null;
    }

}
