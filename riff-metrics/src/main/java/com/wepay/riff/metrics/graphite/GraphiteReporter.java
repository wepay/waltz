package com.wepay.riff.metrics.graphite;

import com.wepay.riff.metrics.core.Clock;
import com.wepay.riff.metrics.core.Counter;
import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.Histogram;
import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.metrics.core.Metered;
import com.wepay.riff.metrics.core.MetricAttribute;
import com.wepay.riff.metrics.core.MetricFilter;
import com.wepay.riff.metrics.core.MetricId;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.core.ScheduledReporter;
import com.wepay.riff.metrics.core.Snapshot;
import com.wepay.riff.metrics.core.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.wepay.riff.metrics.core.MetricAttribute.COUNT;
import static com.wepay.riff.metrics.core.MetricAttribute.M15_RATE;
import static com.wepay.riff.metrics.core.MetricAttribute.M1_RATE;
import static com.wepay.riff.metrics.core.MetricAttribute.M5_RATE;
import static com.wepay.riff.metrics.core.MetricAttribute.MAX;
import static com.wepay.riff.metrics.core.MetricAttribute.MEAN;
import static com.wepay.riff.metrics.core.MetricAttribute.MEAN_RATE;
import static com.wepay.riff.metrics.core.MetricAttribute.MIN;
import static com.wepay.riff.metrics.core.MetricAttribute.P50;
import static com.wepay.riff.metrics.core.MetricAttribute.P75;
import static com.wepay.riff.metrics.core.MetricAttribute.P95;
import static com.wepay.riff.metrics.core.MetricAttribute.P98;
import static com.wepay.riff.metrics.core.MetricAttribute.P99;
import static com.wepay.riff.metrics.core.MetricAttribute.P999;
import static com.wepay.riff.metrics.core.MetricAttribute.STDDEV;

/**
 * A reporter which publishes metric values to a Graphite server.
 */
public final class GraphiteReporter extends ScheduledReporter {

    private static final Logger LOG = LoggerFactory.getLogger(GraphiteReporter.class);

    /**
     * A builder for {@link GraphiteReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static final class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;
        private Set<MetricAttribute> disabledMetricAttributes;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
            this.disabledMetricAttributes = Collections.emptySet();
        }

        /**
         * Specifies whether or not, the executor (used for reporting) will be stopped with same time with reporter.
         * Default value is true.
         * Setting this parameter to false, has the sense in combining with providing external managed executor via {@link #scheduleOn(ScheduledExecutorService)}.
         *
         * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
         * @return {@code this}
         */
        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        /**
         * Specifies the executor to use while scheduling reporting of metrics.
         * Default value is null.
         * Null value leads to executor will be auto created on start.
         *
         * @param executor the executor to use while scheduling reporting of metrics.
         * @return {@code this}
         */
        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Don't report the passed metric attributes for all metrics (e.g. "p999", "stddev" or "m15").
         * See {@link MetricAttribute}.
         *
         * @param disabledMetricAttributes a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
            this.disabledMetricAttributes = disabledMetricAttributes;
            return this;
        }

        /**
         * Builds a {@link GraphiteReporter} with the given properties, sending metrics using the
         * given {@link Graphite}.
         *
         * @param graphiteSender a {@link GraphiteSender}
         * @return a {@link GraphiteReporter}
         */
        public GraphiteReporter build(GraphiteSender graphiteSender) {
            return new GraphiteReporter(registry,
                    graphiteSender,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter,
                    executor,
                    shutdownExecutorOnStop,
                    disabledMetricAttributes);
        }
    }

    private final GraphiteSender graphiteSender;
    private final Clock clock;
    private final String prefix;

    /**
     * Creates a new {@link GraphiteReporter} instance.
     *
     * @param registry               the {@link MetricRegistry} containing the metrics this
     *                               reporter will report
     * @param graphiteSender         the {@link GraphiteSender} which is responsible for sending metrics to server
     *                               via a transport protocol
     * @param clock                  the instance of the time. Use {@link Clock#defaultClock()} for the default
     * @param prefix                 the prefix of all metric names (may be null)
     * @param rateUnit               the time unit of in which rates will be converted
     * @param durationUnit           the time unit of in which durations will be converted
     * @param filter                 the filter for which metrics to report
     * @param executor               the executor to use while scheduling reporting of metrics (may be null).
     * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
     */
    private GraphiteReporter(MetricRegistry registry,
                            GraphiteSender graphiteSender,
                            Clock clock,
                            String prefix,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            ScheduledExecutorService executor,
                            boolean shutdownExecutorOnStop,
                            Set<MetricAttribute> disabledMetricAttributes) {
        super(registry, "graphite-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop,
                disabledMetricAttributes);
        this.graphiteSender = graphiteSender;
        this.clock = clock != null ? clock : Clock.defaultClock();
        this.prefix = prefix;
    }

    /**
     * Returns a new {@link GraphiteReporter.Builder} for {@link GraphiteReporter}.
     *
     * @param registry the registry to report
     * @return a {@link GraphiteReporter.Builder} instance for a {@link GraphiteReporter}
     */
    public static GraphiteReporter.Builder forRegistry(final MetricRegistry registry) {
        return new GraphiteReporter.Builder(registry);
    }

    @Override
    public void report(final SortedMap<MetricId, Gauge> gauges,
                       final SortedMap<MetricId, Counter> counters,
                       final SortedMap<MetricId, Histogram> histograms,
                       final SortedMap<MetricId, Meter> meters,
                       final SortedMap<MetricId, Timer> timers) {
        final long timestamp = clock.getTime() / 1000;

        try {
            graphiteSender.connect();
            for (Map.Entry<MetricId, Gauge> entry : gauges.entrySet()) {
                reportGauge(entry.getKey().getFullName(), entry.getValue(), timestamp);
            }

            for (Map.Entry<MetricId, Counter> entry : counters.entrySet()) {
                reportCounter(entry.getKey().getFullName(), entry.getValue(), timestamp);
            }

            for (Map.Entry<MetricId, Histogram> entry : histograms.entrySet()) {
                reportHistogram(entry.getKey().getFullName(), entry.getValue(), timestamp);
            }

            for (Map.Entry<MetricId, Meter> entry : meters.entrySet()) {
                reportMetered(entry.getKey().getFullName(), entry.getValue(), timestamp);
            }

            for (Map.Entry<MetricId, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey().getFullName(), entry.getValue(), timestamp);
            }
            graphiteSender.flush();
        } catch (IOException e) {
            LOG.warn("Unable to report to graphite.", e);
        } finally {
            try {
                graphiteSender.close();
            } catch (IOException e1) {
                LOG.warn("Error closing Graphite", graphiteSender, e1);
            }
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            try {
                graphiteSender.close();
            } catch (IOException e) {
                LOG.debug("Error disconnecting from Graphite", graphiteSender, e);
            }
        }
    }

    private void reportCounter(String name, Counter counter, long timestamp) throws IOException {
        sendIfEnabled(COUNT, name, counter.getCount(), timestamp);
    }

    private void reportGauge(String name, Gauge<?> gauge, long timestamp) throws IOException {
        final String value = format(gauge.getValue());
        if (value != null) {
            graphiteSender.send(prefix(name), value, timestamp);
        }
    }

    private void reportTimer(String name, Timer timer, long timestamp) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();
        sendIfEnabled(MAX, name, convertDuration(snapshot.getMax()), timestamp);
        sendIfEnabled(MEAN, name, convertDuration(snapshot.getMean()), timestamp);
        sendIfEnabled(MIN, name, convertDuration(snapshot.getMin()), timestamp);
        sendIfEnabled(STDDEV, name, convertDuration(snapshot.getStdDev()), timestamp);
        sendIfEnabled(P50, name, convertDuration(snapshot.getMedian()), timestamp);
        sendIfEnabled(P75, name, convertDuration(snapshot.get75thPercentile()), timestamp);
        sendIfEnabled(P95, name, convertDuration(snapshot.get95thPercentile()), timestamp);
        sendIfEnabled(P98, name, convertDuration(snapshot.get98thPercentile()), timestamp);
        sendIfEnabled(P99, name, convertDuration(snapshot.get99thPercentile()), timestamp);
        sendIfEnabled(P999, name, convertDuration(snapshot.get999thPercentile()), timestamp);
        reportMetered(name, timer, timestamp);
    }

    private void reportMetered(String name, Metered meter, long timestamp) throws IOException {
        sendIfEnabled(COUNT, name, meter.getCount(), timestamp);
        sendIfEnabled(M1_RATE, name, convertRate(meter.getOneMinuteRate()), timestamp);
        sendIfEnabled(M5_RATE, name, convertRate(meter.getFiveMinuteRate()), timestamp);
        sendIfEnabled(M15_RATE, name, convertRate(meter.getFifteenMinuteRate()), timestamp);
        sendIfEnabled(MEAN_RATE, name, convertRate(meter.getMeanRate()), timestamp);
    }

    private void reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();
        sendIfEnabled(COUNT, name, histogram.getCount(), timestamp);
        sendIfEnabled(MAX, name, snapshot.getMax(), timestamp);
        sendIfEnabled(MEAN, name, snapshot.getMean(), timestamp);
        sendIfEnabled(MIN, name, snapshot.getMin(), timestamp);
        sendIfEnabled(STDDEV, name, snapshot.getStdDev(), timestamp);
        sendIfEnabled(P50, name, snapshot.getMedian(), timestamp);
        sendIfEnabled(P75, name, snapshot.get75thPercentile(), timestamp);
        sendIfEnabled(P95, name, snapshot.get95thPercentile(), timestamp);
        sendIfEnabled(P98, name, snapshot.get98thPercentile(), timestamp);
        sendIfEnabled(P99, name, snapshot.get99thPercentile(), timestamp);
        sendIfEnabled(P999, name, snapshot.get999thPercentile(), timestamp);
    }

    private void sendIfEnabled(MetricAttribute type, String name, double value, long timestamp) throws IOException {
        if (getDisabledMetricAttributes().contains(type)) {
            return;
        }
        graphiteSender.send(prefix(name, type.getCode()), format(value), timestamp);
    }

    private void sendIfEnabled(MetricAttribute type, String name, long value, long timestamp) throws IOException {
        if (getDisabledMetricAttributes().contains(type)) {
            return;
        }
        graphiteSender.send(prefix(name, type.getCode()), format(value), timestamp);
    }

    private String format(Object o) {
        if (o instanceof Float) {
            return format(((Float) o).doubleValue());
        } else if (o instanceof Double) {
            return format(((Double) o).doubleValue());
        } else if (o instanceof Byte) {
            return format(((Byte) o).longValue());
        } else if (o instanceof Short) {
            return format(((Short) o).longValue());
        } else if (o instanceof Integer) {
            return format(((Integer) o).longValue());
        } else if (o instanceof Long) {
            return format(((Long) o).longValue());
        } else if (o instanceof BigInteger) {
            return format(((BigInteger) o).doubleValue());
        } else if (o instanceof BigDecimal) {
            return format(((BigDecimal) o).doubleValue());
        } else if (o instanceof Boolean) {
            return format(((Boolean) o) ? 1 : 0);
        }
        return null;
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    private String format(long n) {
        return Long.toString(n);
    }

    private String format(double v) {
        return String.format(Locale.US, "%2.2f", v);
    }
}
