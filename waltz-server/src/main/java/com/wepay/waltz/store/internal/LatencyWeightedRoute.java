package com.wepay.waltz.store.internal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This abstact class represents a route managed by {@link LatencyWeightedRouter}.
 */
public abstract class LatencyWeightedRoute {

    // The expected latency. It computed by the exponential moving average
    private final AtomicLong expectedLatency = new AtomicLong(LatencyWeightedRouter.DEFAULT_LATENCY);

    // The weight. It is defined by an approximation of cumulative latency.
    // Weights are relative. They are comparable only within the same router.
    private final AtomicLong weight = new AtomicLong();

    /**
     * Updates the expected latency using the exponential moving average.
     * @param latency milliseconds
     */
    public void updateExpectedLatency(long latency) {
        if (latency <= 0) {
            latency = 1L;

        } else if (latency > LatencyWeightedRouter.MAX_LATENCY) {
            latency = LatencyWeightedRouter.MAX_LATENCY;
        }

        long oldExpected;
        long newExpected;

        do {
            oldExpected = expectedLatency.get();
            newExpected = (oldExpected + latency) >> 1;

        } while (!expectedLatency.compareAndSet(oldExpected, newExpected));
    }

    /**
     * Updates the weight. The weight is an approximation of cumulative latency.
     */
    public void updateWeight() {
        // Add the current expected latency. This is an approximation of the actual cumulative latency.
        weight.addAndGet(expectedLatency.get());
    }

    public abstract boolean isClosed();

    public long weight() {
        return weight.get();
    }

}
