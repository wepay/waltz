package com.wepay.waltz.common.util;

import com.wepay.zktools.util.Uninterruptibly;

public class BackoffTimer {

    private final Sleeper sleeper = new Sleeper();
    private long maxRetryInterval;

    public BackoffTimer(long maxRetryInterval) {
        this.maxRetryInterval = maxRetryInterval;
    }

    public long backoff(long retryInterval) {
        long interval = Math.min(retryInterval, maxRetryInterval);

        Uninterruptibly.run(sleeper, interval);

        return Math.min(interval * 2, maxRetryInterval);
    }

    public void close() {
        sleeper.close();
    }

    public void setMaxRetryInterval(long maxRetryInterval) {
        this.maxRetryInterval = maxRetryInterval;
    }

    private static class Sleeper implements Uninterruptibly.RunnableWithTimeout {

        private boolean closed = false;

        @Override
        public void run(long remaining) throws Exception {
            synchronized (this) {
                if (!closed) {
                    this.wait(remaining);
                }
            }
        }

        void close() {
            synchronized (this) {
                closed = true;
                // wake up sleeping threads
                this.notifyAll();
            }
        }
    }

}
