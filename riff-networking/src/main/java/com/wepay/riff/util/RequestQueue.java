package com.wepay.riff.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class RequestQueue<T> {

    private final BlockingQueue<T> queue;
    private boolean closed = false;

    public RequestQueue(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    public boolean enqueue(T request) {
        synchronized (this) {
            try {
                while (!closed) {
                    if (queue.offer(request)) {
                        return true;
                    }

                    try {
                        wait();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }
                return false;

            } finally {
                notifyAll();
            }
        }
    }

    public T dequeue() {
        synchronized (this) {
            try {
                T request = null;

                while (!closed) {
                    request = queue.poll();
                    if (request != null) {
                        break;
                    }

                    try {
                        wait();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }
                return request;

            } finally {
                notifyAll();
            }
        }
    }

    public List<T> dequeue(int maxSize) {
        synchronized (this) {
            try {
                while (queue.size() == 0 && !closed) {
                    try {
                        wait();

                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }

                ArrayList<T> batch = null;

                if (!closed) {
                    int size = queue.size();
                    if (size > maxSize) {
                        size = maxSize;
                    }

                    batch = new ArrayList<>(size);

                    for (int i = 0; i < size; i++) {
                        batch.add(queue.poll());
                    }
                }
                return batch;

            } finally {
                notifyAll();
            }
        }
    }

   public List<T> toList() {
        synchronized (this) {
            try {
                ArrayList<T> list = new ArrayList<>(queue.size());
                while (queue.size() > 0) {
                    T request = queue.poll();
                    if (request != null) {
                        list.add(request);
                    }
                }
                return list;

            } finally {
                notifyAll();
            }
        }
    }

    public int size() {
        synchronized (this) {
            return queue.size();
        }
    }

    public void close() {
        synchronized (this) {
            closed = true;
            notifyAll();
        }
    }

}
