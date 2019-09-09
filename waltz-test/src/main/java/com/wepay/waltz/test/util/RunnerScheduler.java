package com.wepay.waltz.test.util;

import com.wepay.riff.util.Logging;
import com.wepay.zktools.util.Uninterruptibly;
import org.slf4j.Logger;

import java.util.concurrent.TimeoutException;


public class RunnerScheduler<S> {

    private static final Logger logger = Logging.getLogger(RunnerScheduler.class);

    protected final String name;
    private final Runner<S> runner;
    private final long upDuration;
    private final long downDuration;
    private final Object lock = new Object();

    private volatile Thread thread;
    private volatile boolean running = false;
    private volatile boolean frozen = false;

    public RunnerScheduler(String name, Runner<S> runner, long upDuration, long downDuration) {
        if (name == null) {
            throw new NullPointerException("name should not be null");
        }
        this.name = name;
        this.runner = runner;
        this.upDuration = upDuration;
        this.downDuration = downDuration;
    }

    public void start() {
        frozen = false;
        synchronized (lock) {
            thread = new Thread() {
                public void run() {
                    try {
                        synchronized (lock) {
                            running = true;

                            while (running) {
                                if (running && !frozen) {
                                    startRunner();
                                }

                                if (running) {
                                    Uninterruptibly.run(lock::wait, upDuration);
                                }

                                if (running && !frozen) {
                                    stopRunner();
                                }

                                if (running) {
                                    Uninterruptibly.run(lock::wait, downDuration);
                                }
                            }
                        }

                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    } finally {
                        stopRunner();
                    }
                }
            };
            thread.setName(thread.getName() + "-RunnerScheduler");
            thread.setDaemon(true);
            thread.start();
        }
    }

    public void stop() throws TimeoutException {
        synchronized (lock) {
            if (running) {
                running = false;
                lock.notifyAll();
            }
        }

        if (thread != null) {
            Uninterruptibly.join(30000, thread);
            thread = null;
        }
    }

    public void freeze() {
        frozen = true;
    }

    public Runner<S> getRunner() {
        return runner;
    }

    private void startRunner() {
        runner.startAsync();
        runner.awaitStart();
        onServerStart();
    }

    private void stopRunner() {
        try {
            runner.stop();
            onServerStop();
        } catch (TimeoutException ex) {
            logger.error(name + " stop failed due to timeout", ex);
        }
    }

    protected void onServerStart() {

    }

    protected void onServerStop() {

    }
}
