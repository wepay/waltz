package com.wepay.riff.network;

import com.wepay.riff.util.PortFinder;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.util.Uninterruptibly;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NetworkClientServerTest {

    @Test
    public void testBasic() throws Exception {
        PortFinder portFinder = new PortFinder();
        int port = portFinder.getPort();

        Map<Short, MessageCodec> codecs = new HashMap<>();

        codecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        EchoServer server = mkServer(port, codecs, Fault.NEVER);
        try {
            ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
            EchoClient client = mkClient(port, codecs, receivingQueue, Fault.NEVER);
            try {
                MockMessage[] messages = new MockMessage[100];
                for (int i = 0; i < messages.length; i++) {
                    messages[i] = new MockMessage("msg" + i);
                }

                Thread thread = new Thread(() -> {
                    for (MockMessage message : messages) {
                        client.sendMessage(message);
                    }
                });

                thread.start();

                for (MockMessage message : messages) {
                    MockMessage reply = Uninterruptibly.call(() -> receivingQueue.poll(1000, TimeUnit.MILLISECONDS));

                    assertNotNull(reply);
                    assertEquals(message.message, reply.message);
                }

            } finally {
                client.close();
            }

        } finally {
            server.close();
        }
    }

    @Test
    public void testOlderClientVersion() throws Exception {
        PortFinder portFinder = new PortFinder();
        int port = portFinder.getPort();

        Map<Short, MessageCodec> serverCodecs = new HashMap<>();
        Map<Short, MessageCodec> clientCodecs = new HashMap<>();

        serverCodecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        serverCodecs.put((short) 1, new MockMessageCodec((byte) 'T', (short) 1));
        serverCodecs.put((short) 2, new MockMessageCodec((byte) 'T', (short) 2));

        clientCodecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        clientCodecs.put((short) 1, new MockMessageCodec((byte) 'T', (short) 1));

        EchoServer server = mkServer(port, serverCodecs, Fault.NEVER);
        try {
            ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
            EchoClient client = mkClient(port, clientCodecs, receivingQueue, Fault.NEVER);
            try {
                MockMessage[] messages = new MockMessage[100];
                for (int i = 0; i < messages.length; i++) {
                    messages[i] = new MockMessage("msg" + i);
                }

                Thread thread = new Thread(() -> {
                    for (MockMessage message : messages) {
                        client.sendMessage(message);
                    }
                });

                thread.start();

                for (MockMessage message : messages) {
                    MockMessage reply = Uninterruptibly.call(() -> receivingQueue.poll(1000, TimeUnit.MILLISECONDS));

                    assertNotNull(reply);
                    assertEquals(message.message, reply.message);
                }

            } finally {
                client.close();
            }
        } finally {
            server.close();
        }
    }

    @Test
    public void testOlderServerVersion() throws Exception {
        PortFinder portFinder = new PortFinder();
        int port = portFinder.getPort();

        Map<Short, MessageCodec> serverCodecs = new HashMap<>();
        Map<Short, MessageCodec> clientCodecs = new HashMap<>();

        serverCodecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        serverCodecs.put((short) 1, new MockMessageCodec((byte) 'T', (short) 1));

        clientCodecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        clientCodecs.put((short) 1, new MockMessageCodec((byte) 'T', (short) 1));
        clientCodecs.put((short) 2, new MockMessageCodec((byte) 'T', (short) 2));

        EchoServer server = mkServer(port, serverCodecs, Fault.NEVER);
        try {
            ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
            EchoClient client = mkClient(port, clientCodecs, receivingQueue, Fault.NEVER);
            try {
                MockMessage[] messages = new MockMessage[100];
                for (int i = 0; i < messages.length; i++) {
                    messages[i] = new MockMessage("msg" + i);
                }

                Thread thread = new Thread(() -> {
                    for (MockMessage message : messages) {
                        client.sendMessage(message);
                    }
                });

                thread.start();

                for (MockMessage message : messages) {
                    MockMessage reply = Uninterruptibly.call(() -> receivingQueue.poll(1000, TimeUnit.MILLISECONDS));

                    assertNotNull(reply);
                    assertEquals(message.message, reply.message);
                }
            } finally {
                client.close();
            }
        } finally {
            server.close();
        }
    }

    @Test
    public void testTooOldServerVersion() throws Exception {
        PortFinder portFinder = new PortFinder();
        int port = portFinder.getPort();

        Map<Short, MessageCodec> serverCodecs = new HashMap<>();
        Map<Short, MessageCodec> clientCodecs = new HashMap<>();

        serverCodecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        serverCodecs.put((short) 1, new MockMessageCodec((byte) 'T', (short) 1));

        clientCodecs.put((short) 2, new MockMessageCodec((byte) 'T', (short) 2));

        EchoServer server = mkServer(port, serverCodecs, Fault.NEVER);
        try {
            ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
            EchoClient client = mkClient(port, clientCodecs, receivingQueue, Fault.NEVER);
            try {
                assertNull(client.getMessageCodec());

            } finally {
                client.close();
            }
        } finally {
            server.close();
        }
    }

    @Test
    public void testTooOldClientVersion() throws Exception {
        PortFinder portFinder = new PortFinder();
        int port = portFinder.getPort();

        Map<Short, MessageCodec> serverCodecs = new HashMap<>();
        Map<Short, MessageCodec> clientCodecs = new HashMap<>();

        serverCodecs.put((short) 1, new MockMessageCodec((byte) 'T', (short) 1));
        serverCodecs.put((short) 2, new MockMessageCodec((byte) 'T', (short) 2));

        clientCodecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));

        EchoServer server = mkServer(port, serverCodecs, Fault.NEVER);
        try {
            ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
            EchoClient client = mkClient(port, clientCodecs, receivingQueue, Fault.NEVER);
            try {
                assertNull(client.getMessageCodec());

            } finally {
                client.close();
            }
        } finally {
            server.close();
        }
    }

    @Test
    public void testNoCommonVersion() throws Exception {
        PortFinder portFinder = new PortFinder();
        int port = portFinder.getPort();

        Map<Short, MessageCodec> serverCodecs = new HashMap<>();
        Map<Short, MessageCodec> clientCodecs = new HashMap<>();

        serverCodecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));
        serverCodecs.put((short) 2, new MockMessageCodec((byte) 'T', (short) 2));

        clientCodecs.put((short) 1, new MockMessageCodec((byte) 'T', (short) 2));
        clientCodecs.put((short) 3, new MockMessageCodec((byte) 'T', (short) 2));

        EchoServer server = mkServer(port, serverCodecs, Fault.NEVER);
        try {
            ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
            EchoClient client = mkClient(port, clientCodecs, receivingQueue, Fault.NEVER);
            try {
                assertNull(client.getMessageCodec());

            } finally {
                client.close();
            }
        } finally {
            server.close();
        }
    }

    @Test
    public void testClientFaults() throws Exception {
        Map<Short, MessageCodec> codecs = new HashMap<>();
        codecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));

        for (Fault fault : Arrays.asList(Fault.BEFORE_HELLO, Fault.AFTER_HELLO, Fault.BEFORE_MESSAGE, Fault.AFTER_MESSAGE)) {
            PortFinder portFinder = new PortFinder();
            int port = portFinder.getPort();

            EchoServer server = mkServer(port, codecs, Fault.NEVER);
            try {
                ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
                EchoClient client = mkClient(port, codecs, receivingQueue, fault);

                try {
                    int echoed = 0;
                    for (int i = 0; i < 100; i++) {
                        MockMessage message = new MockMessage("msg" + i);
                        client.sendMessage(message);

                        if (client.isDisconnected()) {
                            break;
                        }

                        MockMessage reply = Uninterruptibly.call(() -> receivingQueue.poll(10, TimeUnit.MILLISECONDS));
                        while (reply != null) {
                            assertEquals("msg" + echoed, reply.message);
                            echoed++;
                            reply = receivingQueue.poll();
                        }
                    }
                    assertTrue(client.isDisconnected());

                } finally {
                    client.close();
                }

            } finally {
                server.close();
            }
        }

    }

    @Test
    public void testServerFaults() throws Exception {
        Map<Short, MessageCodec> codecs = new HashMap<>();
        codecs.put((short) 0, new MockMessageCodec((byte) 'T', (short) 0));

        for (Fault fault : Arrays.asList(Fault.BEFORE_HELLO, Fault.AFTER_HELLO, Fault.BEFORE_MESSAGE, Fault.AFTER_MESSAGE)) {
            PortFinder portFinder = new PortFinder(7000, 9999);
            int port = portFinder.getPort();

            EchoServer server = mkServer(port, codecs, fault);
            try {
                ArrayBlockingQueue<MockMessage> receivingQueue = new ArrayBlockingQueue<>(100);
                EchoClient client = mkClient(port, codecs, receivingQueue, Fault.NEVER);

                try {
                    int echoed = 0;
                    for (int i = 0; i < 100; i++) {
                        MockMessage message = new MockMessage("msg" + i);
                        client.sendMessage(message);

                        if (client.isDisconnected()) {
                            break;
                        }

                        MockMessage reply = Uninterruptibly.call(() -> receivingQueue.poll(10, TimeUnit.MILLISECONDS));
                        while (reply != null) {
                            assertEquals("msg" + echoed, reply.message);
                            echoed++;
                            reply = receivingQueue.poll();
                        }
                    }
                    assertTrue(client.isDisconnected());

                } finally {
                    client.close();
                }

            } finally {
                server.close();
            }
        }

    }

    private enum ClientState {
        NEW, CONNECTED, DISCONNECTED
    }

    private enum Fault {
        NEVER, BEFORE_HELLO, AFTER_HELLO, BEFORE_MESSAGE, AFTER_MESSAGE
    }

    private static class EchoServer extends NetworkServer {

        private final Map<Short, MessageCodec> codecs;
        private final MessageHandlerCallbacks callbacks;
        private final Fault fault;

        EchoServer(int port, Map<Short, MessageCodec> codecs, MessageHandlerCallbacks callbacks, Fault fault) throws UnknownHostException {
            super(port, null);
            this.codecs = codecs;
            this.callbacks = callbacks;
            this.fault = fault;
        }

        @Override
        protected MessageHandler getMessageHandler() {
            return new EchoServerMessageHandler(codecs, callbacks, fault);
        }

    }

    private abstract static class EchoMessageHandler extends MessageHandler {
        private final Fault fault;

        EchoMessageHandler(Map<Short, MessageCodec> codecs, String helloMessage, MessageHandlerCallbacks callbacks, Fault fault) {
            super(codecs, helloMessage, callbacks, 10, 20);
            this.fault = fault;
        }

        @Override
        protected void sendHello(ChannelHandlerContext ctx) {
            if (fault.equals(Fault.BEFORE_HELLO)) {
                shutdown();
            }

            super.sendHello(ctx);

            if (fault.equals(Fault.AFTER_HELLO)) {
                shutdown();
            }
        }

        @Override
        public boolean sendMessage(Message msg, boolean flush) {
            boolean ret;

            if (fault.equals(Fault.BEFORE_MESSAGE)) {
                shutdown();
            }

            ret = super.sendMessage(msg, flush);

            if (fault.equals(Fault.AFTER_MESSAGE)) {
                shutdown();
            }

            return ret;
        }
    }

    private static class EchoServerMessageHandler extends EchoMessageHandler {
        EchoServerMessageHandler(Map<Short, MessageCodec> codecs, MessageHandlerCallbacks callbacks, Fault fault) {
            super(codecs, "Hello from Server", callbacks, fault);
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

    private EchoServer mkServer(
        final int port,
        final Map<Short, MessageCodec> codecs,
        final Fault fault
    ) throws UnknownHostException {
        return new EchoServer(port, codecs, null, fault);
    }

    private EchoClient mkClient(
        final int port,
        final Map<Short, MessageCodec> codecs,
        final ArrayBlockingQueue<MockMessage> receivingQueue,
        final Fault fault
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
            };
            @Override
            public void onExceptionCaught(Throwable ex) {
                clientState.set(ClientState.DISCONNECTED);
            }
        };

        // Don't use "localhost". Always use InetAddress.getLocalHost().getHostName() for test stability.
        String host = InetAddress.getLocalHost().getHostName();
        EchoClientMessageHandler clientMessageHandler = new EchoClientMessageHandler(codecs, callbacks, receivingQueue, fault);
        EchoClient client = new EchoClient(host, port) {
            @Override
            protected MessageHandler getMessageHandler() {
                return clientMessageHandler;
            }

            public MessageCodec getMessageCodec() {
                return clientMessageHandler.getMessageCodec();
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

    private abstract static class EchoClient extends NetworkClient {

        EchoClient(String host, int port) {
            super(host, port, null);
        }

        @Override
        public boolean sendMessage(Message msg) {
            if (super.sendMessage(msg)) {
                return true;
            } else {
                close();
                return false;
            }
        }

        public abstract MessageCodec getMessageCodec();
    }

    private static class EchoClientMessageHandler extends EchoMessageHandler {
        private final ArrayBlockingQueue<MockMessage> receivingQueue;

        EchoClientMessageHandler(
            final Map<Short, MessageCodec> codecs,
            final MessageHandlerCallbacks callbacks,
            final ArrayBlockingQueue<MockMessage> receivingQueue,
            final Fault fault
        ) {
            super(codecs, "Hello from Client", callbacks, fault);
            this.receivingQueue = receivingQueue;
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

}
