package com.wepay.waltz.test.util;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.IdentityHashMap;
import java.util.Map;

public class ProxyServer {

    private final String remoteHost;
    private final int remotePort;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ChannelFuture channelFuture;

    private final IdentityHashMap<Channel, Channel> channels = new IdentityHashMap<>();

    public ProxyServer(int localPort, String remoteHost, int remotePort) throws UnknownHostException {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(this.bossGroup, this.workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ProxyServerInitializer())
            .childOption(ChannelOption.AUTO_READ, false);

        this.channelFuture = b.bind(InetAddress.getLocalHost(), localPort);
        this.channelFuture.awaitUninterruptibly();
    }

    public void close() {
        channelFuture.addListener((f) -> {
            if (f.isSuccess()) {
                Channel channel = ((ChannelFuture) f).channel();
                ChannelFuture closeFuture;
                if (channel.isOpen()) {
                    closeFuture = channel.close();
                    closeFuture.addListener(f2 -> {
                        bossGroup.shutdownGracefully();
                        workerGroup.shutdownGracefully();
                    });
                }
            }
        });
    }

    public void disconnectAll() {
        synchronized (channels) {
            for (Map.Entry<Channel, Channel> entry : channels.entrySet()) {
                entry.getKey().close().awaitUninterruptibly();
            }
            channels.clear();
        }
    }

    private static void flushAndClose(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private class ProxyServerInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        public void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(new ProxyFrontendHandler());
        }

    }

    private class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

        // As we use inboundChannel.eventLoop() when building the Bootstrap this does not need to be volatile as
        // the outboundChannel will use the same EventLoop (and therefore Thread) as the inboundChannel.
        private Channel outboundChannel;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            final Channel inboundChannel = ctx.channel();

            // Start the connection attempt.
            Bootstrap b = new Bootstrap();
            b.group(inboundChannel.eventLoop())
                .channel(ctx.channel().getClass())
                .handler(new ProxyBackendHandler(inboundChannel))
                .option(ChannelOption.AUTO_READ, false);
            ChannelFuture f = b.connect(remoteHost, remotePort);
            synchronized (this) {
                outboundChannel = f.channel();

                f.addListener(future -> {
                    if (future.isSuccess()) {
                        // connection complete start to read first data
                        inboundChannel.read();
                    } else {
                        // Close the connection if the connection attempt has failed.
                        inboundChannel.close();
                    }
                });

                synchronized (channels) {
                    channels.put(inboundChannel, outboundChannel);
                }
            }
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            synchronized (this) {
                if (outboundChannel.isActive()) {
                    outboundChannel.writeAndFlush(msg).addListener((ChannelFuture future) -> {
                        if (future.isSuccess()) {
                            // was able to flush out data, start to read the next chunk
                            ctx.channel().read();
                        } else {
                            future.channel().close();
                        }
                    });
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            synchronized (this) {
                if (outboundChannel != null) {
                    flushAndClose(outboundChannel);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            flushAndClose(ctx.channel());
        }

    }

    public static class ProxyBackendHandler extends ChannelInboundHandlerAdapter {

        private final Channel inboundChannel;

        ProxyBackendHandler(Channel inboundChannel) {
            this.inboundChannel = inboundChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.read();
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            inboundChannel.writeAndFlush(msg).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            flushAndClose(inboundChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            flushAndClose(ctx.channel());
        }

    }

}
