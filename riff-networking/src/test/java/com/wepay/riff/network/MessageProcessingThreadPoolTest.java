package com.wepay.riff.network;

import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

public class MessageProcessingThreadPoolTest {

    private static final int NUM_MSG_PER_PROCESSOR = 100;

    @Test
    public void testManyThreadsOneProcessor() {
        // numThreads=10, numProcessors=1
        test(10, 1);
    }

    @Test
    public void testOneThreadManyProcessor() {
        // numThreads=1, numProcessors=10
        test(1, 10);
    }

    @Test
    public void testLessThreadsThanProcessors() {
        // numThreads=5, numProcessors=10
        test(5, 10);
    }

    @Test
    public void testMoreThreadThanProcessors() {
        // numThreads=10, numProcessors=5
        test(10, 5);
    }

    @Test
    public void testEqualNumberOfThreadsAndProcessors() {
        // numThreads=10, numProcessors=10
        test(10, 10);
    }

    public void test(int numThreads, int numProcessors) {
        MessageProcessingThreadPool threadPool = new MessageProcessingThreadPool(numThreads).open();
        try {
            TestMessageProcessor[] messageProcessors = new TestMessageProcessor[numProcessors];

            for (int i = 0; i < messageProcessors.length; i++) {
                messageProcessors[i] = new TestMessageProcessor(threadPool);
            }

            List<Integer> list = new ArrayList<>(numProcessors * NUM_MSG_PER_PROCESSOR);
            int[] msgNo = new int[numProcessors];

            for (int i = 0; i < numProcessors; i++) {
                msgNo[i] = 0;
                for (int j = 0; j < NUM_MSG_PER_PROCESSOR; j++) {
                    list.add(i);
                }
            }

            Collections.shuffle(list);

            for (int processorId : list) {
                messageProcessors[processorId].offer(new TestMessage(processorId, msgNo[processorId]));
                msgNo[processorId]++;
            }

            CountDownLatch latch = new CountDownLatch(numProcessors);
            for (int i = 0; i < numProcessors; i++) {
                messageProcessors[i].offer(new EndMessage(latch));
            }

            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }

            for (int processorId = 0; processorId < numProcessors; processorId++) {
                List<TestMessage> actual = messageProcessors[processorId].processedMessages;

                for (TestMessage message : actual) {
                    assertEquals(processorId, message.processorId);
                }

                assertEquals(NUM_MSG_PER_PROCESSOR, actual.size());

                List<TestMessage> expected = new ArrayList<>(NUM_MSG_PER_PROCESSOR);
                for (int messageNumber = 0; messageNumber < NUM_MSG_PER_PROCESSOR; messageNumber++) {
                    expected.add(new TestMessage(processorId, messageNumber));
                }

                assertEquals(expected, actual);
            }
        } finally {
            threadPool.close();
        }
    }

    private static class TestMessageProcessor extends MessageProcessor {

        final List<TestMessage> processedMessages;

        private final Random rand = new Random();

        TestMessageProcessor(MessageProcessingThreadPool threadPool) {
            super(new Throttling(0, 0), threadPool);

            this.processedMessages = new LinkedList<>();
        }

        @Override
        protected void processMessage(Message msg) {
            if (msg instanceof TestMessage) {

                processedMessages.add((TestMessage) msg);
                // Random delay
                Uninterruptibly.sleep(rand.nextInt(5));

            } else {
                ((EndMessage) msg).latch.countDown();
            }
        }
    }

    private static final class TestMessage extends Message {

        private static final byte MESSAGE_TYPE = 10;

        final int processorId;
        final int messageNumber;

        TestMessage(int processorId, int messageNumber) {
            this.processorId = processorId;
            this.messageNumber = messageNumber;
        }

        @Override
        public byte type() {
            return MESSAGE_TYPE;
        }

        @Override
        public String toString() {
            return "[" + processorId + "," + messageNumber + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(processorId, messageNumber);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestMessage
                && ((TestMessage) obj).processorId == processorId
                && ((TestMessage) obj).messageNumber == messageNumber;
        }
    }

    private static class EndMessage extends Message {

        private static final byte MESSAGE_TYPE = 10;

        private final CountDownLatch latch;

        EndMessage(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public byte type() {
            return MESSAGE_TYPE;
        }

    }

}
