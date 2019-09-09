package com.wepay.riff.metrics.core;

/**
 * A filter used to determine whether or not a metric should be reported, among other things.
 */
public interface MetricFilter {
    /**
     * Matches all metrics, regardless of type or name.
     */
    MetricFilter ALL = (id, metric) -> true;

    static MetricFilter startsWith(String prefix) {
        return (id, metric) -> id.getFullName().startsWith(prefix);
    }

    static MetricFilter endsWith(String suffix) {
        return (id, metric) -> id.getFullName().endsWith(suffix);
    }

    static MetricFilter contains(String substring) {
        return (id, metric) -> id.getFullName().contains(substring);
    }

    /**
     * Returns {@code true} if the metric matches the filter; {@code false} otherwise.
     *
     * @param id   the metric's id
     * @param metric the metric
     * @return {@code true} if the metric matches the filter
     */
    boolean matches(MetricId id, Metric metric);
}
