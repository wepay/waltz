package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.storage.exception.StorageException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentFinderTest {

    private static final long FIRST_TRANSACTION_ID = 10;
    private static final long NUM_TRANSACTIONS = 190;
    private static final long LAST_TRANSACTION_ID = FIRST_TRANSACTION_ID + NUM_TRANSACTIONS - 1;
    private static final long SEGMENT_SIZE_THRESHOLD = 10;
    private static final long NUM_SEGMENTS = NUM_TRANSACTIONS % SEGMENT_SIZE_THRESHOLD > 0 ? NUM_TRANSACTIONS / SEGMENT_SIZE_THRESHOLD + 1
                                                                                           : NUM_TRANSACTIONS / SEGMENT_SIZE_THRESHOLD;

    @Test
    public void testFindSegment() throws IOException, StorageException {
        ArrayList<Segment> segments = new ArrayList<>();
        for (long segmentIndex = 0; segmentIndex < NUM_SEGMENTS; segmentIndex++) {
            Segment segment = mock(Segment.class);
            long firstTransactionId = FIRST_TRANSACTION_ID + segmentIndex * SEGMENT_SIZE_THRESHOLD;
            long recordIndex = 0;
            for (; recordIndex < SEGMENT_SIZE_THRESHOLD; recordIndex++) {
                long transactionId = firstTransactionId + recordIndex;
                if (transactionId > LAST_TRANSACTION_ID) {
                    break;
                }
                when(segment.getRecord(transactionId)).thenReturn(new Record(transactionId, mock(ReqId.class), 0, new byte[0], 0));
            }
            when(segment.firstTransactionId()).thenReturn(firstTransactionId);
            when(segment.maxTransactionId()).thenReturn(firstTransactionId + recordIndex - 1);
            segments.add(segment);
        }

        assertEquals(FIRST_TRANSACTION_ID, SegmentFinder.findSegment(segments, FIRST_TRANSACTION_ID).getRecord(FIRST_TRANSACTION_ID).transactionId);
        assertEquals(null, SegmentFinder.findSegment(segments, FIRST_TRANSACTION_ID - 1));

        assertEquals(LAST_TRANSACTION_ID, SegmentFinder.findSegment(segments, LAST_TRANSACTION_ID).getRecord(LAST_TRANSACTION_ID).transactionId);
        assertEquals(null, SegmentFinder.findSegment(segments, LAST_TRANSACTION_ID + 1));

        assertEquals(100, SegmentFinder.findSegment(segments, 100).getRecord(100).transactionId);
        // transaction:100 is in the same segment as transaction:101
        assertEquals(101, SegmentFinder.findSegment(segments, 100).getRecord(101).transactionId);
        // transaction:100 is in different segment from transaction:99
        assertEquals(null, SegmentFinder.findSegment(segments, 100).getRecord(99));
    }

}
