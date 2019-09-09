package com.wepay.riff.network;

import com.wepay.riff.util.RequestQueue;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.StateChangeFuture;
import com.wepay.zktools.util.Uninterruptibly;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public abstract class NetworkClient implements Closeable {

    private enum ClientState {
        NEW, STARTED, CONNECTED, CLOSING, DISCONNECTED
    }

    private final State<ClientState> state = new State<>(ClientState.NEW);

    public final String host;
    public final int port;

    private final SslContext sslCtx;
    private final Object sendBatchLock = new Object();
    private final RequestQueue<Message> requestQueue = new RequestQueue<>(new ArrayBlockingQueue<>(100));
    private final AtomicLong totalMessagesEnq = new AtomicLong(0L);
    private final AtomicLong totalMessagesDeq = new AtomicLong(0L);

    private MessageHandler messageHandler;
    private EventLoopGroup group;
    private ChannelFuture channelFuture;
    private volatile boolean openFailed = false;

    public NetworkClient(String host, int port, SslContext sslCtx) {
        this.host = host;
        this.port = port;
        this.sslCtx = sslCtx;
        this.messageHandler = null;
    }

    protected abstract MessageHandler getMessageHandler();

    public void openAsync() {
        synchronized (state) {
            if (!state.is(ClientState.NEW)) {
                return;
            }
            state.set(ClientState.STARTED);
            messageHandler = getMessageHandler();
            group = new NioEventLoopGroup();
        }

        synchronized (this) {
            Bootstrap b = new Bootstrap();
            b.group(group)
                .option(ChannelOption.TCP_NODELAY, true)
                .channel(NioSocketChannel.class)
                .handler(new ClientInitializer());

            // Make a new connection asynchronously
            channelFuture = b.connect(host, port);
            channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    state.compareAndSet(ClientState.STARTED, ClientState.CONNECTED);
                } else {
                    openFailed = true;
                    shutdown();
                }
            });
        }
    }

    public void open() {
        openAsync();
        ensureOpen();
    }

    private void ensureOpen() {
        if (!state.is(ClientState.CONNECTED)) {
            StateChangeFuture<ClientState> watch = state.watch();
            while (watch.currentState == ClientState.NEW || watch.currentState == ClientState.STARTED) {
                try {
                    watch.get();
                    watch = state.watch();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                } catch (ExecutionException ex) {
                    // Ignore
                }
            }

            if (watch.currentState != ClientState.CONNECTED) {
                throw new ConnectFailedException("failed to connect: " + host + ":" + port);
            }
        }
    }

    public void closeAsync() {
        synchronized (this) {
            if (isValid()) {
                state.set(ClientState.CLOSING);

                channelFuture.addListener(f -> {
                    if (f.isSuccess()) {
                        Channel channel = ((ChannelFuture) f).channel();
                        if (channel.isOpen()) {
                            channel.close().addListener(f2 -> shutdown());
                        } else {
                            shutdown();
                        }
                    }
                });
            }
        }
    }

    @Override
    public void close() {
        closeAsync();
        Uninterruptibly.run(() -> state.await(ClientState.DISCONNECTED));
    }

    public boolean openFailed() {
        return openFailed;
    }

    public boolean isValid() {
        return !state.is(ClientState.CLOSING) && !state.is(ClientState.DISCONNECTED);
    }

    public boolean isDisconnected() {
        return state.is(ClientState.DISCONNECTED);
    }

    protected void shutdown() {
        synchronized (state) {
            if (!state.is(ClientState.DISCONNECTED)) {
                state.set(ClientState.DISCONNECTED);
                if (group != null) {
                    group.shutdownGracefully();
                }
            }
        }
        requestQueue.close();
    }

    public boolean sendMessage(Message msg) {
        if (requestQueue.enqueue(msg)) {
            long position = totalMessagesEnq.incrementAndGet();

            try {
                ensureOpen();

            } catch (ConnectFailedException ex) {
                return false;
            }

            synchronized (sendBatchLock) {
                if (totalMessagesDeq.get() < position) {
                    // Batch up requests in one flush
                    int numMessagesToSend = requestQueue.size();

                    while (numMessagesToSend > 0) {
                        numMessagesToSend--;
                        Message message = requestQueue.dequeue();
                        if (message != null) {
                            totalMessagesDeq.incrementAndGet();
                            messageHandler.ensureWritable();
                            if (!messageHandler.sendMessage(message, numMessagesToSend == 0)) {
                                return false;
                            }
                        } else {
                            // The queue is closed. It means the network client is disconnected.
                            return false;
                        }
                    }
                }
            }

            return true;

        } else {
            // The queue is closed. It means the network client is disconnected.
            return false;
        }
    }

    private class ClientInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();

            if (sslCtx != null) {
                pipeline.addLast(sslCtx.newHandler(ch.alloc(), host, port));
            }

            // Add the message codec
            pipeline.addLast("helloDecoder", new MessageDecoder(HelloCodec.INSTANCE));
            pipeline.addLast("helloEncoder", new MessageEncoder(HelloCodec.INSTANCE));

            // and then the message handler
            pipeline.addLast((ChannelHandler) messageHandler);
        }
    }

}
