package com.wepay.waltz.test.util;

import com.wepay.zktools.util.Uninterruptibly;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public abstract class Runner<S> {

    private static final long THREAD_JOIN_TIMEOUT = 20000;

    protected CountDownLatch serverStarted = null;
    protected CountDownLatch done = null;
    protected volatile Thread thread = null;
    protected S server = null;

    private volatile Throwable serverException = null;

    public void startAsync() {
        synchronized (this) {
            if (thread != null) {
                throw new IllegalStateException("runner already started");
            }

            serverStarted = new CountDownLatch(1);
            done = new CountDownLatch(1);

            thread = createThread();
            thread.start();
        }

    }

    public void stop() throws TimeoutException {
        synchronized (this) {
            if (thread != null) {
                done.countDown();
                Uninterruptibly.join(THREAD_JOIN_TIMEOUT, thread);
                thread = null;
            }
        }
    }

    public S awaitStart() {
        synchronized (this) {
            if (thread == null) {
                throw new IllegalStateException("runner not started");
            }
            Uninterruptibly.run(serverStarted::await);

            if (server == null) {
                if (serverException != null) {
                    throw new IllegalStateException("server not started", serverException);

                } else {
                    throw new IllegalStateException("server not started");
                }
            }

            return server;
        }
    }

    protected abstract S createServer() throws Exception;

    protected abstract void closeServer();

    private Thread createThread() {
        Thread t = new Thread() {
            public void run() {
                try {
                    server = createServer();
                    serverStarted.countDown();
                    Uninterruptibly.run(done::await);
                    closeServer();

                } catch (Throwable ex) {
                    // Make sure the latch is released on error
                    serverStarted.countDown();
                    serverException = ex;
                }
            }
        };

        String runnerName = this.getClass().getSimpleName();

        t.setName(t.getName() + "-" + runnerName);
        t.setDaemon(true);

        return t;
    }

}
