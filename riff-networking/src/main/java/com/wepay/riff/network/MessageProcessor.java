package com.wepay.riff.network;

import com.wepay.riff.util.Logging;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MessageProcessor maintains a queue of messages. It accepts a new message ({@link #offer(Message)}) and append it
 * to the queue. An instance of MessageProcessor submits itself to {@link MessageProcessingThreadPool} when there are
 * messages to process. {@link MessageProcessingThreadPool} calls {@link #processMessage()} to process a next
 * available message.
 */
public abstract class MessageProcessor {

    private static final Logger logger = Logging.getLogger(MessageProcessor.class);

    private static final int PROCESSOR_DEQUEUED = 0;
    private static final int PROCESSOR_ENQUEUED = 1;
    private static final int PROCESSOR_RUNNING = 2;
    private static final int PROCESSOR_CLOSED = -1;

    private final ReentrantLock lock = new ReentrantLock();
    private final Throttling throttling;
    private final MessageProcessingThreadPool threadPool;
    private final LinkedList<Message> messageQueue;

    private int state;

    MessageProcessor(Throttling throttling, MessageProcessingThreadPool threadPool) {
        this.throttling = throttling;
        this.threadPool = threadPool;
        this.messageQueue = new LinkedList<>();
        this.state = PROCESSOR_DEQUEUED;
    }

    /**
     * Closes the processor. Messages in the message queue will be cleared, and no message processing will take place.
     */
    void close() {
        synchronized (messageQueue) {
            messageQueue.clear();
            state = PROCESSOR_CLOSED;
        }
    }

    /**
     * Adds a new message to the message queue. This method ensures that this processor is resubmitted to
     * {@link MessageProcessingThreadPool}.
     *
     * @param msg message
     */
    void offer(Message msg) {
        synchronized (messageQueue) {
            if (state != PROCESSOR_CLOSED) {
                throttling.increment();
                messageQueue.offer(msg);

                // There are more message to process. Enqueue the processor iff dequeued.
                if (state == PROCESSOR_DEQUEUED) {
                    threadPool.submit(this);
                    state = PROCESSOR_ENQUEUED;
                }
            }
        }
    }

    /**
     * Processes a message in the message queue. This method ensures that this processor is resubmitted to
     * {@link MessageProcessingThreadPool} if there are more messages to process.
     */
    void processMessage() {
        // Makes sure that there is no concurrent processing.
        lock.lock();
        try {
            Message msg;

            synchronized (messageQueue) {
                if (state == PROCESSOR_CLOSED) {
                    // Do nothing because the processor is already closed.
                    return;
                }

                if (state != PROCESSOR_ENQUEUED) {
                    logger.error("invalid processor state in processMessage:" + state);
                }

                msg = messageQueue.poll();
                state = PROCESSOR_RUNNING;
            }

            if (msg != null) {
                try {
                    processMessage(msg);
                } finally {
                    throttling.decrement();
                    tryEnqueue();
                }
            } else {
                tryEnqueue();
            }
        } finally {
            lock.unlock();
        }
    }

    private void tryEnqueue() {
        synchronized (messageQueue) {
            if (messageQueue.isEmpty()) {
                // There is no message to process.
                state = PROCESSOR_DEQUEUED;

            } else {
                // There are more message to process. Enqueue the processor if it is not closed yet.
                if (state != PROCESSOR_CLOSED) {
                    threadPool.submit(this);
                    state = PROCESSOR_ENQUEUED;
                }
            }
        }
    }

    protected abstract void processMessage(Message msg);

}
