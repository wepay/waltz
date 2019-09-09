package com.wepay.riff.metrics.core;

import java.util.EventListener;

/**
 * Listeners for events from the registry.  Listeners must be thread-safe.
 */
public interface MetricRegistryListener extends EventListener {
    /**
     * A no-op implementation of {@link MetricRegistryListener}.
     */
    abstract class Base implements MetricRegistryListener {
        @Override
        public void onGaugeAdded(MetricId id, Gauge<?> gauge) {
        }

        @Override
        public void onGaugeRemoved(MetricId id) {
        }

        @Override
        public void onCounterAdded(MetricId id, Counter counter) {
        }

        @Override
        public void onCounterRemoved(MetricId id) {
        }

        @Override
        public void onHistogramAdded(MetricId id, Histogram histogram) {
        }

        @Override
        public void onHistogramRemoved(MetricId id) {
        }

        @Override
        public void onMeterAdded(MetricId id, Meter meter) {
        }

        @Override
        public void onMeterRemoved(MetricId id) {
        }

        @Override
        public void onTimerAdded(MetricId id, Timer timer) {
        }

        @Override
        public void onTimerRemoved(MetricId id) {
        }
    }

    /**
     * Called when a {@link Gauge} is added to the registry.
     *
     * @param id  the gauge's id
     * @param gauge the gauge
     */
    void onGaugeAdded(MetricId id, Gauge<?> gauge);

    /**
     * Called when a {@link Gauge} is removed from the registry.
     *
     * @param id the gauge's id
     */
    void onGaugeRemoved(MetricId id);

    /**
     * Called when a {@link Counter} is added to the registry.
     *
     * @param id    the counter's id
     * @param counter the counter
     */
    void onCounterAdded(MetricId id, Counter counter);

    /**
     * Called when a {@link Counter} is removed from the registry.
     *
     * @param id the counter's id
     */
    void onCounterRemoved(MetricId id);

    /**
     * Called when a {@link Histogram} is added to the registry.
     *
     * @param id      the histogram's id
     * @param histogram the histogram
     */
    void onHistogramAdded(MetricId id, Histogram histogram);

    /**
     * Called when a {@link Histogram} is removed from the registry.
     *
     * @param id the histogram's id
     */
    void onHistogramRemoved(MetricId id);

    /**
     * Called when a {@link Meter} is added to the registry.
     *
     * @param id  the meter's id
     * @param meter the meter
     */
    void onMeterAdded(MetricId id, Meter meter);

    /**
     * Called when a {@link Meter} is removed from the registry.
     *
     * @param id the meter's id
     */
    void onMeterRemoved(MetricId id);

    /**
     * Called when a {@link Timer} is added to the registry.
     *
     * @param id  the timer's id
     * @param timer the timer
     */
    void onTimerAdded(MetricId id, Timer timer);

    /**
     * Called when a {@link Timer} is removed from the registry.
     *
     * @param id the timer's id
     */
    void onTimerRemoved(MetricId id);
}
