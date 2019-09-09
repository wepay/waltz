package com.wepay.riff.metrics.core;

import java.util.Map;

/**
 * A set of named metrics.
 *
 * @see MetricRegistry#registerAll(MetricSet)
 */
public interface MetricSet extends Metric {
    /**
     * A map of metric ids to metrics.
     *
     * @return the metrics
     */
    Map<MetricId, Metric> getMetrics();
}
