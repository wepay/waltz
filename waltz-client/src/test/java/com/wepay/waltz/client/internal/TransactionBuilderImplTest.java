package com.wepay.waltz.client.internal;

import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.test.mock.MockContext;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TransactionBuilderImplTest {

    @Test
    public void test() {
        MockContext context = MockContext.builder()
            .data("good transaction")
            .writeLocks(1, 2, 3)
            .readLocks(4, 5, 6)
            .appendLocks(7, 8, 9)
            .build();

        TransactionBuilderImpl builder = new TransactionBuilderImpl(new ReqId(111, 222, 333, 444), 999);

        context.execute(builder);

        AppendRequest request = builder.buildRequest();

        assertEquals(new ReqId(111, 222, 333, 444), request.reqId);
        assertEquals(999, request.clientHighWaterMark);

        assertEquals("good transaction", new String(request.data, StandardCharsets.UTF_8));

        int[] expectedWriteLockRequest = new int[]{
            MockContext.makeLock(1).hashCode(),
            MockContext.makeLock(2).hashCode(),
            MockContext.makeLock(3).hashCode()
        };

        int[] expectedReadLockRequest = new int[]{
            MockContext.makeLock(4).hashCode(),
            MockContext.makeLock(5).hashCode(),
            MockContext.makeLock(6).hashCode()
        };

        int[] expectedAppendLockRequest = new int[]{
            MockContext.makeLock(7).hashCode(),
            MockContext.makeLock(8).hashCode(),
            MockContext.makeLock(9).hashCode()
        };

        assertArrayEquals(expectedWriteLockRequest, request.writeLockRequest);
        assertArrayEquals(expectedReadLockRequest, request.readLockRequest);
        assertArrayEquals(expectedAppendLockRequest, request.appendLockRequest);
    }

}
