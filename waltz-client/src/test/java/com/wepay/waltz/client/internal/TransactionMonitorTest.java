package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.TransactionBuilder;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.exception.ClientClosedException;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.exception.PartitionInactiveException;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionMonitorTest {

    private static final long timeout = 100;

    private TransactionMonitor monitor;
    private TransactionContext context; // dummy

    @Before
    public void setup() {
        monitor = new TransactionMonitor(WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS);
        context = new TransactionContext() {
            @Override
            public int partitionId(int numPartitions) {
                return 0;
            }

            @Override
            public boolean execute(TransactionBuilder builder) {
                return true;
            }
        };
    }

    @After
    public void teardown() {
        monitor.close();
    }

    @Test
    public void testRegister() throws Exception {
        monitor.start();

        ReqId reqId1 = new ReqId(0, 1);
        ReqId reqId2 = new ReqId(0, 2);
        ReqId reqId3 = new ReqId(0, 3);
        ReqId reqId4 = new ReqId(0, 4);

        TransactionFuture future1 = monitor.register(reqId1, context, timeout);
        assertFalse(future1.isDone());

        // Duplicate reqId
        TransactionFuture futureDup = monitor.register(reqId1, context, timeout);
        assertTrue(futureDup.isDone());
        assertFalse(futureDup.get());

        TransactionFuture future2 = monitor.register(reqId2, context, timeout);
        assertFalse(future2.isDone());

        TransactionFuture future3 = monitor.register(reqId3, context, timeout);
        assertFalse(future3.isDone());

        TransactionFuture future4 = monitor.register(reqId4, context, timeout);
        assertFalse(future3.isDone());

        monitor.committed(reqId1);

        assertTrue(future1.isDone());
        assertFalse(future2.isDone());
        assertFalse(future3.isDone());
        assertFalse(future4.isDone());

        monitor.committed(reqId4);

        assertTrue(future2.isDone());
        assertTrue(future3.isDone());
        assertTrue(future4.isDone());

        assertFalse(future2.get());
        assertFalse(future3.get());
        assertTrue(future4.get());
    }

    @Test
    public void testTransactionContext() {
        monitor.start();

        ReqId reqId = new ReqId(0, 1);

        TransactionFuture future1 = monitor.register(reqId, context, timeout);
        assertEquals(context, future1.transactionContext);
        assertEquals(context, monitor.getTransactionContext(reqId));
        assertEquals(context, monitor.committed(reqId));
    }

    @Test
    public void testSuspendResume() throws Exception {
        ReqId reqId1 = new ReqId(0, 1);
        ReqId reqId2 = new ReqId(0, 2);

        // This should fail because the initial state is STOPPED.
        assertFalse(monitor.stop());

        // Start the monitor
        assertTrue(monitor.start());
        assertFalse(monitor.isStopped());

        // This should fail because the monitor has been started.
        assertFalse(monitor.start());

        // Stop the monitor.
        assertTrue(monitor.stop());
        assertTrue(monitor.isStopped());

        // A stopped monitor should return an exceptionally completed future.
        TransactionFuture future = monitor.register(reqId1, context, timeout);
        assertTrue(future.isCompletedExceptionally());

        try {
            future.get();

        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof PartitionInactiveException);
        }

        monitor.start();

        TransactionFuture future1 = monitor.register(reqId1, context, timeout);
        assertFalse(future1.isDone());

        TransactionFuture future2 = monitor.register(reqId2, context, timeout);
        assertFalse(future2.isDone());

        monitor.committed(reqId1);
        assertTrue(future1.isDone());
        assertFalse(future2.isDone());

        monitor.stop();

        monitor.committed(reqId2);
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    @Test
    public void testRegisterTimeout() throws Exception {
        monitor.start();

        int capacity = monitor.maxCapacity();

        for (int i = 0; i < capacity; i++) {
            TransactionFuture future = monitor.register(new ReqId(0, i), context, timeout);
            assertNotNull(future);
            assertFalse(future.isDone());
        }

        for (int i = 0; i < 3; i++) {
            TransactionFuture future = monitor.register(new ReqId(0, capacity + i), context, timeout);
            assertNull(future);
        }
    }

    @Test
    public void testAbort() throws Exception {
        monitor.start();

        final int capacity = monitor.maxCapacity();
        ArrayList<ReqId> reqIds = new ArrayList<>();
        HashMap<ReqId, TransactionFuture> futures = new HashMap<>();

        for (int i = 0; i < capacity; i++) {
            reqIds.add(new ReqId(0, i));
            futures.put(new ReqId(0, i), monitor.register(new ReqId(0, i), context, timeout));
        }

        int remaining = capacity;
        Collections.shuffle(reqIds);
        for (ReqId reqId : reqIds) {
            monitor.abort(reqId);
            remaining--;

            assertFalse(futures.get(reqId).get());
            assertFalse(futures.get(reqId).isFlushed());

            int pendingCount = 0;
            for (TransactionFuture future : futures.values()) {
                if (!future.isDone()) {
                    pendingCount++;
                }
            }

            assertEquals(remaining, pendingCount);
            assertEquals(remaining, monitor.registeredCount());
        }
    }

    @Test
    public void testFlush() throws Exception {
        monitor.start();

        final int capacity = monitor.maxCapacity();
        ArrayList<ReqId> reqIds = new ArrayList<>();
        ArrayList<TransactionFuture> futures = new ArrayList<>();

        for (int i = 0; i < capacity; i++) {
            reqIds.add(new ReqId(0, i));
            futures.add(monitor.register(new ReqId(0, i), context, timeout));
        }

        int aborted = capacity / 2;
        monitor.abort(reqIds.get(aborted));

        int flushed = capacity / 2 - 1;
        monitor.flush(reqIds.get(flushed));

        int remaining = 0;
        for (int i = 0; i < capacity; i++) {
            if (i <= flushed) {
                assertFalse(futures.get(i).get());
                assertTrue(futures.get(i).isFlushed());
            } else {
                if (i == aborted) {
                    assertFalse(futures.get(i).get());
                } else {
                    // not completed yet
                    assertFalse(futures.get(i).isDone());
                    remaining++;
                }
                assertFalse(futures.get(i).isFlushed());
            }
        }
        assertEquals(remaining, monitor.registeredCount());

        flushed = aborted;
        monitor.flush(reqIds.get(flushed));

        remaining = 0;
        for (int i = 0; i < capacity; i++) {
            if (i <= flushed) {
                assertFalse(futures.get(i).get());
                assertTrue(futures.get(i).isFlushed());
            } else {
                // not completed yet
                assertFalse(futures.get(i).isDone());
                assertFalse(futures.get(i).isFlushed());
                remaining++;
            }
        }
        assertEquals(remaining, monitor.registeredCount());

        flushed = capacity / 2 + 1;
        monitor.flush(reqIds.get(flushed));

        remaining = 0;
        for (int i = 0; i < capacity; i++) {
            if (i <= flushed) {
                assertFalse(futures.get(i).get());
                assertTrue(futures.get(i).isFlushed());
            } else {
                // not completed yet
                assertFalse(futures.get(i).isDone());
                assertFalse(futures.get(i).isFlushed());
                remaining++;
            }
        }
        assertEquals(remaining, monitor.registeredCount());

        flushed = capacity - 1;
        monitor.flush(reqIds.get(flushed));

        for (int i = 0; i < capacity; i++) {
            assertFalse(futures.get(i).get());
            assertTrue(futures.get(i).isFlushed());
        }
        assertEquals(0, monitor.registeredCount());
    }

    @Test
    public void testClose() throws Exception {
        monitor.start();

        ReqId reqId1 = new ReqId(0, 1);
        ReqId reqId2 = new ReqId(1, 2);
        ReqId reqId3 = new ReqId(2, 3);
        ReqId reqId4 = new ReqId(3, 4);

        TransactionFuture future1 = monitor.register(reqId1, context, timeout);
        assertFalse(future1.isDone());

        TransactionFuture future2 = monitor.register(reqId2, context, timeout);
        assertFalse(future2.isDone());

        TransactionFuture future3 = monitor.register(reqId3, context, timeout);
        assertFalse(future3.isDone());

        TransactionFuture future4 = monitor.register(reqId4, context, timeout);
        assertFalse(future4.isDone());

        monitor.committed(reqId1);
        monitor.committed(reqId2);

        assertTrue(future1.isDone());
        assertTrue(future1.get());
        assertTrue(future1.isFlushed());
        assertTrue(future2.isDone());
        assertTrue(future2.get());
        assertTrue(future2.isFlushed());

        assertFalse(future3.isDone());
        assertFalse(future3.isFlushed());
        assertFalse(future4.isDone());
        assertFalse(future4.isFlushed());

        monitor.close();

        assertTrue(future3.isDone());
        assertTrue(future3.isFlushed());
        try {
            future3.get();
            fail();
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof ClientClosedException);
        } catch (Throwable ex) {
            fail();
        }

        assertTrue(future4.isDone());
        assertTrue(future4.isFlushed());
        try {
            future4.get();
            fail();
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof ClientClosedException);
        } catch (Throwable ex) {
            fail();
        }
    }

    @Test
    public void testMaxConcurrentTransactions() throws Exception {
        monitor.start();

        final int capacity = monitor.maxCapacity();
        final int numReqIds = 1000;

        State<Integer> totalTransactions = new State<>(0);

        Thread writer = new Thread(() -> {
            for (int i = 0; i < numReqIds; i++) {
                ReqId reqId = new ReqId(0, i);
                monitor.register(reqId, context, timeout);
                totalTransactions.set(i + 1);
            }
        });
        writer.start();

        Thread reader = new Thread(() -> {
            Random rand = new Random();
            boolean writerRunning = true;
            int index = -1;

            for (int i = 0; i < numReqIds; i++) {
                // Sleep randomly
                if (rand.nextInt(20) == 0) {
                    Uninterruptibly.sleep(5);
                }

                while (writerRunning) {
                    StateChangeFuture<Integer> future = totalTransactions.watch();
                    index = future.currentState;

                    if (index == numReqIds) {
                        writerRunning = false;
                        break;
                    }

                    if (index >= i + capacity) {
                        break;
                    }

                    try {
                        future.get();
                    } catch (Exception ex) {
                        // Ignore
                    }
                }
                //assertTrue(index <= i + capacity);
                monitor.committed(new ReqId(0, i));
            }
        });
        reader.start();

        Uninterruptibly.join(10000, writer, reader);
    }

    @Test
    public void testLastEnqueued() {
        monitor.start();

        long req1Start = System.currentTimeMillis();
        TransactionFuture req1Future = monitor.register(new ReqId(0, 1), context, timeout);
        long req1Done = System.currentTimeMillis();

        assertEquals(req1Future, monitor.lastEnqueued());
        assertTrue(req1Start <= monitor.lastEnqueuedTime());
        assertTrue(monitor.lastEnqueuedTime() <= req1Done);

        Uninterruptibly.sleep(3);
        long req2Start = System.currentTimeMillis();
        TransactionFuture req2Future = monitor.register(new ReqId(0, 2), context, timeout);
        long req2Done = System.currentTimeMillis();

        assertEquals(req2Future, monitor.lastEnqueued());
        assertTrue(req2Start <= monitor.lastEnqueuedTime());
        assertTrue(monitor.lastEnqueuedTime() <= req2Done);

        monitor.committed(new ReqId(0, 1));

        assertEquals(req2Future, monitor.lastEnqueued());
        assertTrue(req2Start <= monitor.lastEnqueuedTime());
        assertTrue(monitor.lastEnqueuedTime() <= req2Done);

        monitor.committed(new ReqId(0, 2));

        assertNull(monitor.lastEnqueued());
    }

    @Test
    public void testClear() throws Exception {
        monitor.start();

        final int numReqIds = 1000;

        State<Integer> totalTransactions = new State<>(0);

        Thread writer = new Thread(() -> {
            for (int i = 0; i < numReqIds; i++) {
                ReqId reqId = new ReqId(0, i);
                monitor.register(reqId, context, timeout);
                totalTransactions.set(i + 1);
            }
        });

        Thread reader = new Thread(() -> {
            Random rand = new Random();
            int index = -1;

            for (int i = 0; i < numReqIds; i++) {
                // Sleep randomly
                if (rand.nextInt(20) == 0) {
                    Uninterruptibly.sleep(5);
                }

                while (true) {
                    StateChangeFuture<Integer> future = totalTransactions.watch();
                    index = future.currentState;

                    if (index > i) {
                        break;
                    }

                    try {
                        future.get();
                    } catch (Exception ex) {
                        // Ignore
                    }
                }
                monitor.committed(new ReqId(0, i));
            }
        });

        Thread cleaner = new Thread(() -> {
            Random rand = new Random();
            int index = -1;

            for (int i = 0; i < numReqIds; i++) {
                // Clear randomly
                if (rand.nextInt(20) == 0) {
                    Uninterruptibly.sleep(5);
                    monitor.clear();
                }

                while (true) {
                    StateChangeFuture<Integer> future = totalTransactions.watch();
                    index = future.currentState;

                    if (index  > i) {
                        break;
                    }

                    try {
                        future.get();
                    } catch (Exception ex) {
                        // Ignore
                    }
                }
            }
        });

        cleaner.start();
        writer.start();
        reader.start();

        Uninterruptibly.join(10000, writer, reader, cleaner);

        assertEquals(0, monitor.registeredCount());
    }

}
