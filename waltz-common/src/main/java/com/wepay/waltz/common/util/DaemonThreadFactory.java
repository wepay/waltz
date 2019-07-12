package com.wepay.waltz.common.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DaemonThreadFactory implements ThreadFactory {

    public static final ThreadFactory INSTANCE = new DaemonThreadFactory();

    private final ThreadFactory delegate = Executors.defaultThreadFactory();

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = delegate.newThread(r);
        thread.setDaemon(true);

        return thread;
    }

}
