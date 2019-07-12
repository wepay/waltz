package com.wepay.waltz.common.util;

import com.wepay.riff.util.RepeatingTask;
import com.wepay.riff.util.RequestQueue;

import java.util.concurrent.CompletableFuture;

public abstract class QueueConsumerTask<E> extends RepeatingTask {

    private final RequestQueue<E> queue;

    protected QueueConsumerTask(String taskName, RequestQueue<E> queue) {
        super(taskName);
        this.queue = queue;
    }

    public boolean enqueue(E item) {
        return queue.enqueue(item);
    }

    public int queueSize() {
        return queue.size();
    }

    protected void task() throws Exception {
        E item = queue.dequeue();
        if (isRunning()) {
            if (item != null) {
                process(item);
            } else {
                idle();
            }
        }
    }

    protected abstract void process(E item) throws Exception;

    protected void idle() throws Exception {
        // A subclass may override this.
    }

    @Override
    public CompletableFuture<Boolean> stop() {
        CompletableFuture<Boolean> future = super.stop();
        queue.close();
        return future;
    }
}

