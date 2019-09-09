package com.wepay.riff.network;

import com.wepay.zktools.util.Uninterruptibly;
import io.netty.channel.ChannelConfig;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessageProcessorTest {

    private static final int LOW_THRESHOLD = 10;
    private static final int HIGH_THRESHOLD = 20;

    @Test
    public void test() {
        Exchanger<Message> exchanger = new Exchanger<>();
        Throttling throttling = new Throttling(LOW_THRESHOLD, HIGH_THRESHOLD);
        AtomicBoolean autoRead = new AtomicBoolean();
        throttling.setChannelConfig(mockChannelConfig(autoRead));

        MessageProcessingThreadPool threadPool = new MessageProcessingThreadPool(1).open();
        try {
            MessageProcessor messageProcessor = new MessageProcessor(throttling, threadPool) {
                @Override
                protected void processMessage(Message msg) {
                    while (true) {
                        try {
                            exchanger.exchange(msg);
                            break;

                        } catch (InterruptedException ex) {
                            Thread.interrupted();
                        }
                    }
                }
            };

            for (int i = 0; i < HIGH_THRESHOLD * 2; i++) {
                MockMessage message = new MockMessage("msg" + i);

                assertEquals(i, throttling.size());

                messageProcessor.offer(message);

                assertEquals(i + 1, throttling.size());

                if (throttling.size() <= HIGH_THRESHOLD) {
                    assertTrue(autoRead.get());
                } else {
                    assertFalse(autoRead.get());
                }
            }

            int remaining = HIGH_THRESHOLD * 2;
            for (int i = 0; i < HIGH_THRESHOLD * 2; i++) {
                MockMessage message;
                while (true) {
                    try {
                        message = (MockMessage) exchanger.exchange(null);
                        Thread.yield();
                        break;

                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }

                assertEquals("msg" + i, message.message);

                // wait for the counter in throttling goes down.
                for (int retry = 0; retry < 10; retry++) {
                    if (throttling.size() < remaining) {
                        break;
                    }
                    Uninterruptibly.sleep(10);
                }

                assertEquals(--remaining, throttling.size());

                if (throttling.size() < LOW_THRESHOLD) {
                    assertTrue(autoRead.get());
                } else {
                    assertFalse(autoRead.get());
                }
            }

            assertEquals(0, throttling.size());

        } finally {
            threadPool.close();
        }
    }

    private ChannelConfig mockChannelConfig(final AtomicBoolean autoRead) {
        return (ChannelConfig) Proxy.newProxyInstance(this.getClass().getClassLoader(),
            new Class[] {
                ChannelConfig.class
            },
            new InvocationHandler() {
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if (method.getName().equals("setAutoRead")) {
                        autoRead.set((boolean) args[0]);
                    }
                    return null;
                }
            }
        );
    }

}
