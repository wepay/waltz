package com.wepay.riff.util;

import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RequestQueueTest {

    private static final int QUEUE_SIZE = 10;
    private static final int NUM = 100;
    private static final int BATCH_SIZE = 3;
    private static final long SLEEP = 10;
    private static final long LONG_SLEEP = 100;

    @Test
    public void testSingleDequeue() {
        RequestQueue<Integer> queue = new RequestQueue<>(new ArrayBlockingQueue<>(QUEUE_SIZE));

        CountDownLatch latch = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            Uninterruptibly.run(latch::await);
            for (int i = 0; i < NUM; i++) {
                if (i < NUM / 2) {
                    Uninterruptibly.sleep(SLEEP);
                }

                queue.enqueue(i);
            }
        });

        ArrayList<Integer> dequeued = new ArrayList<>();

        Thread reader = new Thread(() -> {
            Uninterruptibly.run(latch::await);

            for (int i = 0; i < NUM; i++) {
                if (i > NUM / 2) {
                    Uninterruptibly.sleep(SLEEP);
                }

                dequeued.add(queue.dequeue());
            }
        });

        writer.start();
        reader.start();

        Uninterruptibly.sleep(LONG_SLEEP);
        latch.countDown();

        Uninterruptibly.run(writer::join);
        Uninterruptibly.run(reader::join);

        assertEquals(0, queue.size());
        assertEquals(NUM, dequeued.size());
        for (int i = 0; i < NUM; i++) {
            assertEquals(i, (int) dequeued.get(i));
        }
    }

    @Test
    public void testBatchDequeue() {
        RequestQueue<Integer> queue = new RequestQueue<>(new ArrayBlockingQueue<>(QUEUE_SIZE));

        CountDownLatch latch = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            Uninterruptibly.run(latch::await);
            for (int i = 0; i < NUM; i++) {
                if (i < NUM / 2) {
                    Uninterruptibly.sleep(SLEEP);
                }

                queue.enqueue(i);
            }
        });

        ArrayList<Integer> batchSizes = new ArrayList<>();
        ArrayList<Integer> dequeued = new ArrayList<>();

        Thread reader = new Thread(() -> {
            Uninterruptibly.run(latch::await);

            List<Integer> batch = queue.dequeue(BATCH_SIZE);
            while (batch != null && dequeued.size() < NUM) {
                if (batch.size() > 0 && batch.get(0) > NUM / 2) {
                    Uninterruptibly.sleep(SLEEP);
                }

                batchSizes.add(batch.size());
                dequeued.addAll(batch);

                if (dequeued.size() < NUM) {
                    batch = queue.dequeue(BATCH_SIZE);
                }
            }
        });

        writer.start();
        reader.start();

        Uninterruptibly.sleep(LONG_SLEEP);
        latch.countDown();

        Uninterruptibly.run(writer::join);
        Uninterruptibly.run(reader::join);

        assertEquals(0, queue.size());
        assertEquals(NUM, dequeued.size());
        for (int i = 0; i < NUM; i++) {
            assertEquals(i, (int) dequeued.get(i));
        }

        boolean ok = false;
        for (int batchSize : batchSizes) {
            assertNotEquals(0, batchSize);
            if (batchSize > 1) {
                ok = true;
            }
        }

        assertTrue(ok);
    }

    @Test
    public void testToList() {
        RequestQueue<Integer> queue = new RequestQueue<>(new ArrayBlockingQueue<>(QUEUE_SIZE));

        for (int size = 0; size < QUEUE_SIZE; size++) {
            for (int i = 0; i < size; i++) {
                queue.enqueue(i);
            }
            List<Integer> list = queue.toList();
            assertEquals(size, list.size());
            for (int i = 0; i < size; i++) {
                assertEquals(i, (int) list.get(i));
            }
        }
    }

    @Test
    public void testClose() {
        RequestQueue<Integer> queue = new RequestQueue<>(new ArrayBlockingQueue<>(QUEUE_SIZE));

        CountDownLatch latch = new CountDownLatch(1);

        AtomicReference<Integer> ret0 = new AtomicReference<>(-1);
        AtomicReference<List<Integer>> ret1 = new AtomicReference<>(Collections.singletonList(-1));

        Thread reader0 = new Thread(() -> {
            Uninterruptibly.run(latch::await);
            ret0.set(queue.dequeue());
        });

        Thread reader1 = new Thread(() -> {
            Uninterruptibly.run(latch::await);
            ret1.set(queue.dequeue(BATCH_SIZE));
        });

        reader0.start();
        reader1.start();

        Uninterruptibly.sleep(LONG_SLEEP);

        latch.countDown();

        Uninterruptibly.sleep(LONG_SLEEP);

        queue.close();

        Uninterruptibly.run(reader0::join);
        Uninterruptibly.run(reader1::join);

        assertEquals(0, queue.size());
        assertNull(ret0.get());
        assertNull(ret1.get());

        assertNull(queue.dequeue());
        assertNull(queue.dequeue(BATCH_SIZE));
    }

}
