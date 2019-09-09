package com.wepay.riff.network;

import io.netty.channel.ChannelConfig;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Throttling is used by MessageProcessor to control network traffic according to the number of messages in a buffer.
 * An instance of Throttling is created for each MessageHandler and shared by all MessageProcessor associated with it.
 * It toggles the auto read flag through {@link ChannelConfig}. When the auto read is off, netty won't read next message.
 * Throttling keep the message count, and MessageProcessor
 * increments the count by calling {@link #increment()} when a new message is added
 * and decrements the count by calling {@link #decrement()} when a message is processed.
 */
public class Throttling {

    private final int lowThreshold;
    private final int highThreshold;
    private final AtomicBoolean throttled = new AtomicBoolean(false);

    private int bufferedMessageCount;
    private ChannelConfig channelConfig;

    Throttling(int lowThreshold, int highThreshold) {
        this.lowThreshold = lowThreshold;
        this.highThreshold = highThreshold;
        this.bufferedMessageCount = 0;
    }

    /**
     * Sets the ChannelConfig. ChannelConfig is used to toggle the auto read flag according to the buffered message count
     * @param channelConfig
     */
    void setChannelConfig(ChannelConfig channelConfig) {
        synchronized (this) {
            this.channelConfig = channelConfig;
            this.channelConfig.setAutoRead(true);
            this.throttled.set(false);
        }
    }

    /**
     * Increments the buffered message count.
     */
    void increment() {
        synchronized (this) {
            if (++bufferedMessageCount > highThreshold) {
                throttle(true);
            }
        }
    }

    /**
     * Increments the buffered message count.
     */
    void decrement() {
        synchronized (this) {
            if (--bufferedMessageCount < lowThreshold) {
                throttle(false);
            }
        }
    }

    /**
     * Returns the buffered message count.
     */
    int size() {
        synchronized (this) {
            return bufferedMessageCount;
        }
    }

    private void throttle(boolean throttle) {
        if (channelConfig != null) {
            if (throttled.compareAndSet(!throttle, throttle)) {
                channelConfig.setAutoRead(!throttle);
            }
        }
    }

}
