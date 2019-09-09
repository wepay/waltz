package com.wepay.riff.network;

import com.wepay.riff.util.Logging;
import com.wepay.zktools.util.State;
import com.wepay.zktools.util.Uninterruptibly;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;

import javax.net.ssl.SSLHandshakeException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class MessageHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger logger = Logging.getLogger(MessageHandler.class);

    private static final int WRITE_BUFFER_LOW_WATER_MARK = 16 * 1024;
    private static final int WRITE_BUFFER_HIGH_WATER_MARK = 32 * 1024;
    private static final WriteBufferWaterMark WRITE_BUFFER_WATER_MARK =
        new WriteBufferWaterMark(WRITE_BUFFER_LOW_WATER_MARK, WRITE_BUFFER_HIGH_WATER_MARK);
    private static final Integer DEFAULT_PROCESSOR_ID = 0;

    private final Object writeLock = new Object();
    private final MessageHandlerCallbacks callbacks;
    private final State<Boolean> writable = new State<>(false);
    private final Uninterruptibly.Runnable awaitWritable = () -> writable.await(true);
    private final GenericFutureListener<ChannelFuture> writeCompletionListener;
    private final Throttling throttling;
    private final MessageProcessingThreadPool threadPool;
    private final Map<Integer, MessageProcessor> inboundQueues = new HashMap<>();
    private final boolean hasPrivateThreadPool;

    private volatile ChannelHandlerContext ctx;
    private volatile MessageCodec messageCodec = null;

    private final Map<Short, MessageCodec> codecs;
    private final String helloMessage;

    public MessageHandler(
        Map<Short, MessageCodec> codecs,
        String helloMessage,
        MessageHandlerCallbacks callbacks,
        int queueLowThreshold,
        int queueHighThreshold
    ) {
        this(
            codecs,
            helloMessage,
            callbacks,
            queueLowThreshold,
            queueHighThreshold,
            null
        );
    }

    public MessageHandler(
        Map<Short, MessageCodec> codecs,
        String helloMessage,
        MessageHandlerCallbacks callbacks,
        int queueLowThreshold,
        int queueHighThreshold,
        MessageProcessingThreadPool threadPool
    ) {
        super();

        this.codecs = codecs;
        this.helloMessage = helloMessage;

        this.throttling = new Throttling(queueLowThreshold, queueHighThreshold);
        if (threadPool == null) {
            this.hasPrivateThreadPool = true;
            this.threadPool = new MessageProcessingThreadPool(1);
            // The private thread pool is not opened here. It is delayed until channelActive() to avoid thread leak.
        } else {
            this.hasPrivateThreadPool = false;
            this.threadPool = threadPool;
        }

        this.callbacks = callbacks;

        // A listener for write operations. This increments the capacity when a write request completes.
        this.writeCompletionListener = (f -> {
            if (!f.isSuccess()) {
                logger.debug("write failed, shutting down", f.cause());
                shutdown();
            }
        });
    }

    @Override
    public final void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        if (hasPrivateThreadPool) {
            this.threadPool.open();
        }

        this.ctx = ctx;
        ctx.channel().config().setWriteBufferWaterMark(WRITE_BUFFER_WATER_MARK);
        this.throttling.setChannelConfig(ctx.channel().config());
        this.writable.set(true);

        // Say Hello!
        sendHello(ctx);
    }

    @Override
    public final void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (this.hasPrivateThreadPool) {
            this.threadPool.close();
        }

        for (MessageProcessor messageProcessor : inboundQueues.values()) {
            messageProcessor.close();
        }

        this.ctx = null;
        this.writable.set(true); // unblock threads waiting in ensureWritable

        super.channelInactive(ctx);

        if (callbacks != null) {
            callbacks.onChannelInactive();
        }
    }

    @Override
    public final void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (msg.type() == Hello.MESSAGE_TYPE) {
            // Hello message
            Hello hello = (Hello) msg;

            logger.debug("received Hello: message=[{}] from={}", hello.message, ctx.channel());

            if (messageCodec != null) {
                HandshakeFailedException ex = new HandshakeFailedException("duplicate hello messages");

                logger.error("duplicate hello messages", ex);

                shutdown();
                throw ex;
            }

            messageCodec = findCodec(hello.versions);
            if (messageCodec != null) {
                ctx.pipeline().replace("helloDecoder", "decoder", new MessageDecoder(messageCodec));
                ctx.pipeline().replace("helloEncoder", "encoder", new MessageEncoder(messageCodec));

                // We declare the channel is active after we get Hello message. This is different from Netty terminology.
                // TODO : revisit, change terminology
                if (callbacks != null) {
                    callbacks.onChannelActive();
                }

            } else {
                HandshakeFailedException ex = new HandshakeFailedException("failed to find a common codec version");
                logger.error("failed to find a common codec version", ex);

                shutdown();
                throw ex;
            }

            initialize();

        } else {
            // All other messages
            MessageProcessor processor = getMessageProcessor(extractProcessorId(msg));
            processor.offer(msg);
        }
    }

    private MessageProcessor getMessageProcessor(Integer processorId) {
        synchronized (inboundQueues) {
            MessageProcessor messageProcessor = inboundQueues.get(processorId);

            if (messageProcessor == null) {
                messageProcessor = new MessageProcessor(throttling, threadPool) {
                    @Override
                    protected void processMessage(Message msg) {
                        try {
                            process(msg);
                        } catch (Throwable t) {
                            shutdown();
                        }
                    }
                };
                inboundQueues.put(processorId, messageProcessor);
            }

            return messageProcessor;
        }
    }

    @Override
    public final void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
        if (ex instanceof SSLHandshakeException) {
            logger.error(
                "exception caught: handler={}, SSL handshake failed {}",
                this.getClass().getName(),
                ctx.channel()
            );
        } else if (!(ex instanceof DisconnectedException)) {
            logger.error("exception caught: handler={}, channel={}", this.getClass().getName(), ctx.channel(), ex);
        }

        shutdown();

        if (callbacks != null) {
            try {
                callbacks.onExceptionCaught(ex);
            } catch (Throwable t) {
                // Ignore
            }
        }
    }

    @Override
    public final void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);

        boolean isWritable = ctx.channel().isWritable();
        writable.set(isWritable);

        synchronized (writeLock) {
            callbacks.onWritabilityChanged(isWritable);

            if (!isWritable) {
                ctx.flush();
            }
        }
    }

    public boolean isActive() {
        return ctx != null;
    }

    protected void initialize() throws Exception {
        // A subclass may perform some initialization here.
    }

    protected Integer extractProcessorId(Message msg) {
        return DEFAULT_PROCESSOR_ID;
    }

    protected abstract void process(Message msg) throws Exception;

    protected void sendHello(ChannelHandlerContext ctx) {
        logger.debug("sending Hello: message=[{}] to={}", helloMessage, ctx.channel());
        ctx.writeAndFlush(new Hello(codecs.keySet(), helloMessage)).addListener(writeCompletionListener);
    }

    public boolean sendMessage(Message msg, boolean flush) {
        synchronized (writeLock) {
            ChannelHandlerContext ctx = this.ctx; // for safety
            if (ctx != null) {
                if (messageCodec == null) {
                    // The handshake has not been completed
                    return false;
                }

                if (flush) {
                    ctx.writeAndFlush(msg).addListener(writeCompletionListener);
                } else {
                    ctx.write(msg).addListener(writeCompletionListener);
                }

                return true;

            } else {
                // The channel is not ready or is disconnected, ignore the request.
                logger.debug("channel is not ready or is disconnected, not sending message");
                return false;
            }
        }
    }

    public boolean isWritable() {
        return writable.get();
    }

    public void flush() {
        ChannelHandlerContext ctx = this.ctx; // for safety
        if (ctx != null) {
            ctx.flush();
        }
    }

    MessageCodec getMessageCodec() {
        return messageCodec;
    }

    public final void shutdown() {
        if (this.hasPrivateThreadPool) {
            this.threadPool.close();
        }

        for (MessageProcessor messageProcessor: inboundQueues.values()) {
            messageProcessor.close();
        }

        ChannelHandlerContext ctx = this.ctx;
        if (ctx != null) {
            ctx.close();
        }
    }

    private MessageCodec findCodec(Set<Short> clientVersions) {
        Short maxCommonVersion = Short.MIN_VALUE;
        for (Short clientVersion : clientVersions) {
            if (clientVersion > maxCommonVersion && codecs.containsKey(clientVersion)) {
                maxCommonVersion = clientVersion;
            }
        }
        return codecs.get(maxCommonVersion);
    }

    public void ensureWritable() {
        while (!isWritable()) {
            // The buffer may be full. Force flushing.
            flush();

            // Wait until the channel becomes writable again
            try {
                Uninterruptibly.run(awaitWritable);
            } catch (Uninterruptibly.InvocationException ex) {
                // Ignore
            }
        }
    }

}
