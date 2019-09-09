package com.wepay.riff.network;

import io.netty.channel.ChannelConfig;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThrottlingTest {

    private static final int LOW_THRESHOLD = 10;
    private static final int HIGH_THRESHOLD = 20;

    @Test
    public void test() {
        Throttling throttling = new Throttling(LOW_THRESHOLD, HIGH_THRESHOLD);
        AtomicBoolean autoRead = new AtomicBoolean();
        throttling.setChannelConfig(mockChannelConfig(autoRead));

        for (int i = 0; i < HIGH_THRESHOLD * 2; i++) {
            assertEquals(i, throttling.size());

            throttling.increment();

            assertEquals(i + 1, throttling.size());

            if (throttling.size() <= HIGH_THRESHOLD) {
                assertTrue(autoRead.get());
            } else {
                assertFalse(autoRead.get());
            }
        }

        int remaining = HIGH_THRESHOLD * 2;
        for (int i = 0; i < HIGH_THRESHOLD * 2; i++) {
            assertEquals(remaining, throttling.size());

            throttling.decrement();

            assertEquals(--remaining, throttling.size());

            if (throttling.size() < LOW_THRESHOLD) {
                assertTrue(autoRead.get());
            } else {
                assertFalse(autoRead.get());
            }
        }

        assertEquals(0, throttling.size());
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
