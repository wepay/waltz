package com.wepay.riff.network;

import com.wepay.riff.util.Logging;
import com.wepay.riff.util.RepeatingTask;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.Queue;

/**
 * MessageProcessingThreadPool is a thread pool specialized for message processing.
 * An instance of MessageProcessingThreadPool can be shared by multiple {@link MessageHandler}s.
 */
public class MessageProcessingThreadPool {

    private static final Logger logger = Logging.getLogger(MessageProcessingThreadPool.class);

    private final MessageProcessingTask[] tasks;
    private final Queue<MessageProcessor> processorQueue;

    private boolean closed = false;

    /**
     * Creates the thread pool with the specified number of threads.
     * @param numThreads number of threads in processing pool
     */
    public MessageProcessingThreadPool(int numThreads) {
        if (numThreads <= 0) {
            throw new IllegalArgumentException("the number of thread must be positive");
        }
        this.processorQueue = new LinkedList<>();

        this.tasks = new MessageProcessingTask[numThreads];
        for (int i = 0; i < numThreads; i++) {
            tasks[i] = new MessageProcessingTask("MessageProcessingTask" + i);
        }
    }

    /**
     * Opens this thread pool.
     * @return returns itself so it can be used functionally
     */
    public MessageProcessingThreadPool open() {
        for (MessageProcessingTask task : tasks) {
            task.start();
        }
        return this;
    }

    /**
     * Closes this thread pool. All threads will be stopped.
     */
    public void close() {
        synchronized (processorQueue) {
            closed = true;
            processorQueue.clear();
            for (MessageProcessingTask task : tasks) {
                try {
                    while (task != null && task.isRunning()) {
                        task.stop();
                    }
                } catch (Throwable ex) {
                    // Ignore
                }
            }
            processorQueue.notifyAll();
        }
    }

    /**
     * Submits a message processor to this thread pool. It throws {@link MessageProcessingThreadPoolClosedException)
     * if the thread pool is already closed.
     * @param messageProcessor
     */
    void submit(MessageProcessor messageProcessor) {
        synchronized (processorQueue) {
            if (closed) {
                throw new MessageProcessingThreadPoolClosedException();
            }
            processorQueue.offer(messageProcessor);
            processorQueue.notifyAll();
        }
    }

    private class MessageProcessingTask extends RepeatingTask {

        MessageProcessingTask(String taskName) {
            super(taskName);
        }

        protected void task() {
            MessageProcessor processor = pollNextProcessor();
            if (processor != null) {
                // Process one message. The processor will submits itself if it has more messages to process.
                processor.processMessage();
            }
            // else the thread pool is shutting down
        }

        private MessageProcessor pollNextProcessor() {
            synchronized (processorQueue) {
                while (!closed) {
                    MessageProcessor processor = processorQueue.poll();
                    if (processor != null) {
                        return processor;
                    } else {
                        try {
                            processorQueue.wait();
                        } catch (InterruptedException ex) {
                            Thread.interrupted();
                        }
                    }
                }
                return null;
            }
        }

        @Override
        protected void exceptionCaught(Throwable ex) {
            logger.error("exception caught", ex);
        }

    }

}
