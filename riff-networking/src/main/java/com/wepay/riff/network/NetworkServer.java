package com.wepay.riff.network;

import com.wepay.zktools.util.State;
import com.wepay.zktools.util.Uninterruptibly;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;

public abstract class NetworkServer implements Closeable {

    private enum ServerState {
        STARTED, CLOSING, CLOSED
    }

    private final State<ServerState> state = new State<>(ServerState.STARTED);

    private final SslContext sslCtx;
    private final EventLoopGroup parentGroup;
    private final EventLoopGroup childGroup;
    private final ChannelFuture channelFuture;

    public NetworkServer(int port, SslContext sslCtx) throws UnknownHostException {
        this.sslCtx = sslCtx;
        this.parentGroup = new NioEventLoopGroup(1);
        this.childGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
            .option(ChannelOption.TCP_NODELAY, true)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ServerInitializer());
        this.channelFuture = b.bind(InetAddress.getLocalHost(), port);
        this.channelFuture.addListener(f -> {
            if (!f.isSuccess()) {
                state.set(ServerState.CLOSING);
                shutdown();
            }
        });
        this.channelFuture.awaitUninterruptibly();
    }

    protected abstract MessageHandler getMessageHandler();

    @Override
    public void close() {
        if (state.compareAndSet(ServerState.STARTED, ServerState.CLOSING)) {
            // Use a listener to close the port since it may not be opened yet
            channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    Channel channel = ((ChannelFuture) f).channel();
                    if (channel.isOpen()) {
                        // We will shutdown after the port is closed
                        channel.close().addListener(f2 -> shutdown());
                    } else {
                        shutdown();
                    }
                }
            });
        }
        Uninterruptibly.run(() -> state.await(ServerState.CLOSED));
    }

    public boolean isClosed() {
        return state.is(ServerState.CLOSED);
    }

    private void shutdown() {
        if (state.compareAndSet(ServerState.CLOSING, ServerState.CLOSED)) {
            parentGroup.shutdownGracefully();
            childGroup.shutdownGracefully();
        }
    }

    private class ServerInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();

            if (sslCtx != null) {
                pipeline.addLast(sslCtx.newHandler(ch.alloc()));
            }

            // Add the message codec
            pipeline.addLast("helloDecoder", new MessageDecoder(HelloCodec.INSTANCE));
            pipeline.addLast("helloEncoder", new MessageEncoder(HelloCodec.INSTANCE));

            // and then the message handler
            pipeline.addLast(getMessageHandler());
        }

    }

}
