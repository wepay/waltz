package com.wepay.riff.network;

import com.wepay.riff.util.PortFinder;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MessageHandlerProcessorTest {

    // Throttling thresholds
    private static final int QUEUE_LOW_THRESHOLD = 10;
    private static final int QUEUE_HIGH_THRESHOLD = 50;

    private static final int NUM_MESSAGES = 1000;

    @Test
    public void testSingleProcessorSingleThread() throws Exception {
        test(1, 1);
    }

    @Test
    public void testMultipleProcessorsSingleThread() throws Exception {
        test(10, 1);
    }

    @Test
    public void testSingleProcessorMultipleThreads() throws Exception {
        test(1, 5);
    }

    @Test
    public void testMultipleProcessorsMultipleThreads() throws Exception {
        test(10, 5);
    }

    public void test(int numProcessors, int numThreads) throws Exception {
        Random rand = new Random();

        MessageProcessingThreadPool threadPool = new MessageProcessingThreadPool(numThreads).open();

        PortFinder portFinder = new PortFinder();
        int port = portFinder.getPort();

        Map<Short, MessageCodec> codecs = new HashMap<>();

        codecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        NetworkServer server = mkServer(port, codecs);
        try {
            ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(NUM_MESSAGES);
            NetworkClient client = mkClient(port, codecs, receivingQueue, numProcessors, threadPool);
            try {
                MockMessage[] messages = new MockMessage[NUM_MESSAGES];
                for (int i = 0; i < messages.length; i++) {
                    messages[i] = new MockMessage(String.format("msg%08d-%03d", i, rand.nextInt(1000)));
                }

                Thread thread = new Thread(() -> {
                    for (MockMessage message : messages) {
                        client.sendMessage(message);
                    }
                });

                thread.start();

                // Generate the expected result. The result should preserve the ordering within a processor
                Map<Integer, LinkedList<MockMessage>> expected = new HashMap<>();
                ArrayList<String> expectedReplyText = new ArrayList<>(NUM_MESSAGES);
                for (MockMessage message : messages) {
                    Integer processorId = message.message.hashCode() % numProcessors;
                    LinkedList<MockMessage> list = expected.computeIfAbsent(processorId, key -> new LinkedList<>());
                    list.add(message);
                    expectedReplyText.add(message.message);
                }

                // The actual result should not violate ordering within a processor
                ArrayList<String> allReplyText = new ArrayList<>(NUM_MESSAGES);
                while (!expected.isEmpty()) {
                    MockMessage reply = Uninterruptibly.call(() -> receivingQueue.poll(3000, TimeUnit.MILLISECONDS));
                    assertNotNull(reply);

                    allReplyText.add(reply.message);

                    Integer processorId = reply.message.hashCode() % numProcessors;

                    LinkedList<MockMessage> list = expected.get(processorId);
                    assertNotNull(list);

                    MockMessage message = list.poll();
                    assertEquals(message.message, reply.message);

                    if (list.isEmpty()) {
                        expected.remove(processorId);
                    }
                }

                if (numProcessors == 1) {
                    assertEquals(expectedReplyText, allReplyText);
                }

            } finally {
                client.close();
            }

        } finally {
            server.close();
            threadPool.close();
        }
    }

    private NetworkServer mkServer(final int port, final Map<Short, MessageCodec> codecs) throws UnknownHostException {

        MessageHandlerCallbacks callbacks = new MessageHandlerCallbacks() {
            @Override
            public void onChannelActive() {
            }
            @Override
            public void onChannelInactive() {
            }
            @Override
            public void onWritabilityChanged(boolean isWritable) {
            }
            @Override
            public void onExceptionCaught(Throwable ex) {
                ex.printStackTrace();
            }
        };

        return new NetworkServer(port, null) {
            @Override
            protected MessageHandler getMessageHandler() {
                return new TestServerMessageHandler(codecs, callbacks);
            }
        };
    }

    private enum ClientState {
        NEW, CONNECTED, DISCONNECTED
    }

    private NetworkClient mkClient(
        final int port,
        final Map<Short, MessageCodec> codecs,
        final ArrayBlockingQueue<MockMessage> receivingQueue,
        final int numProcessors,
        final MessageProcessingThreadPool threadPool
    ) throws Exception {
        State<ClientState> clientState = new State<>(ClientState.NEW);

        MessageHandlerCallbacks callbacks = new MessageHandlerCallbacks() {
            @Override
            public void onChannelActive() {
                clientState.set(ClientState.CONNECTED);
            }
            @Override
            public void onChannelInactive() {
                clientState.set(ClientState.DISCONNECTED);
            }
            @Override
            public void onWritabilityChanged(boolean isWritable) {
                // DO nothing
            }
            @Override
            public void onExceptionCaught(Throwable ex) {
                clientState.set(ClientState.DISCONNECTED);
                ex.printStackTrace();
            }
        };

        MessageHandler clientMessageHandler =
            new TestClientMessageHandler(codecs, callbacks, receivingQueue, numProcessors, threadPool);

        // Don't use "localhost". Always use InetAddress.getLocalHost().getHostName() for test stability.
        NetworkClient client = new NetworkClient(InetAddress.getLocalHost().getHostName(), port, null) {
            @Override
            protected MessageHandler getMessageHandler() {
                return clientMessageHandler;
            }
        };

        client.open();

        try {
            // Wait until connected (or disconnected)
            StateChangeFuture<ClientState> watch = clientState.watch();
            while (watch.currentState == ClientState.NEW) {
                Uninterruptibly.run(watch::get);
                watch = clientState.watch();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            // Ignore
        }

        return client;
    }


    private static class TestClientMessageHandler extends MessageHandler {

        private final ArrayBlockingQueue<MockMessage> receivingQueue;
        private final int numProcessors;

        TestClientMessageHandler(
            final Map<Short, MessageCodec> codecs,
            final MessageHandlerCallbacks callbacks,
            final ArrayBlockingQueue<MockMessage> receivingQueue,
            final int numProcessors,
            final MessageProcessingThreadPool threadPool
        ) {
            super(codecs, "Hello from Client", callbacks, QUEUE_LOW_THRESHOLD, QUEUE_HIGH_THRESHOLD, threadPool);
            this.receivingQueue = receivingQueue;
            this.numProcessors = numProcessors;
        }

        @Override
        protected Integer extractProcessorId(Message msg) {
            return ((MockMessage) msg).message.hashCode() % numProcessors;
        }

        @Override
        protected void process(Message msg) throws Exception {
            switch (msg.type()) {
                case MockMessage.MESSAGE_TYPE:
                    receivingQueue.add((MockMessage) msg);
                    break;

                default:
                    throw new IllegalArgumentException("message not handled: msg=" + msg);
            }
        }
    }

    private static class TestServerMessageHandler extends MessageHandler {
        TestServerMessageHandler(Map<Short, MessageCodec> codecs, MessageHandlerCallbacks callbacks) {
            super(codecs, "Hello from Server", callbacks, QUEUE_LOW_THRESHOLD, QUEUE_HIGH_THRESHOLD);
        }

        @Override
        protected void process(Message msg) throws Exception {
            switch (msg.type()) {
                case MockMessage.MESSAGE_TYPE:
                    sendMessage(msg, true);
                    break;

                default:
                    throw new IllegalArgumentException("message not handled: msg=" + msg);
            }
        }
    }

}
