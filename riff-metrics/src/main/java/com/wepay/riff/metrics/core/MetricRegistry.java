package com.wepay.riff.metrics.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A registry of metric instances.
 */
// suppress warning to help unit test, as Mockito cannot mock final class
@SuppressWarnings("checkstyle:finalclass")
public class MetricRegistry implements MetricSet {
    /**
     * Concatenates elements to form a dotted group, eliding any null values or empty strings.
     *
     * @param group  the first element of the domain
     * @param groups the remaining elements of the name
     * @return {@code name} and {@code names} concatenated by periods
     */
    public static String group(String group, String... groups) {
        final StringBuilder builder = new StringBuilder();
        append(builder, group);
        if (groups != null) {
            for (String s : groups) {
                append(builder, s);
            }
        }
        return builder.toString();
    }

    /**
     * Concatenates a class name and elements to form a dotted group, eliding any null values or
     * empty strings.
     *
     * @param klass the first element of the group
     * @param groups the remaining elements of the groups
     * @return {@code klass} and {@code names} concatenated by periods
     */
    public static String group(Class<?> klass, String... groups) {
        return name(klass.getName(), groups);
    }

    /**
     * Concatenates elements to form a dotted name, eliding any null values or empty strings.
     *
     * @param name  the first element of the name
     * @param names the remaining elements of the name
     * @return {@code name} and {@code names} concatenated by periods
     */
    public static String name(String name, String... names) {
        final StringBuilder builder = new StringBuilder();
        append(builder, name);
        if (names != null) {
            for (String s : names) {
                append(builder, s);
            }
        }
        return builder.toString();
    }

    /**
     * Concatenates a class name and elements to form a dotted name, eliding any null values or
     * empty strings.
     *
     * @param klass the first element of the name
     * @param names the remaining elements of the name
     * @return {@code klass} and {@code names} concatenated by periods
     */
    public static String name(Class<?> klass, String... names) {
        return name(klass.getName(), names);
    }

    private static void append(StringBuilder builder, String part) {
        if (part != null && !part.isEmpty()) {
            if (builder.length() > 0) {
                builder.append('.');
            }
            builder.append(part);
        }
    }

    /**
     * Return a lazy-loaded singleton MetricRegistry instance with a safe and highly concurrent
     * lazy initialization with Initialization-on-demand holder idiom.
     *
     * @return a singleton {@link MetricRegistry}
     */
    public static MetricRegistry getInstance() {
        return MetricRegistryHolder.INSTANCE;
    }

    public void register(String group, String name, RatioGauge ratioGauge) {
    }

    private static class MetricRegistryHolder {
        private static final MetricRegistry INSTANCE = new MetricRegistry();
    }

    private final ConcurrentMap<MetricId, Metric> metrics;
    private final List<MetricRegistryListener> listeners;

    /**
     * Creates a new {@link MetricRegistry}.
     */
    private MetricRegistry() {
        this.metrics = buildMap();
        this.listeners = new CopyOnWriteArrayList<>();
    }

    /**
     * Creates a new {@link ConcurrentMap} implementation for use inside the registry. Override this
     * to create a {@link MetricRegistry} with space- or time-bounded metric lifecycles, for
     * example.
     *
     * @return a new {@link ConcurrentMap}
     */
    protected ConcurrentMap<MetricId, Metric> buildMap() {
        return new ConcurrentHashMap<>();
    }

    /**
     * Given a {@link Metric}, registers it under the given group and name.
     *
     * @param group the group of the metric
     * @param name   the name of the metric
     * @param metric the metric
     * @param <T>    the type of the metric
     * @return {@code metric}
     * @throws IllegalArgumentException if the id is already registered
     */
    @SuppressWarnings("unchecked")
    public <T extends Metric> T register(String group, String name, T metric) throws IllegalArgumentException {
        if (metric instanceof MetricSet) {
            registerAll(name(group, name), (MetricSet) metric);
        } else {
            final MetricId id = new MetricId(group, name);
            final Metric existing = metrics.putIfAbsent(id, metric);
            if (existing == null) {
                onMetricAdded(id, metric);
            } else {
                throw new IllegalArgumentException("A metric id " + id + " already exists");
            }
        }
        return metric;
    }

    /**
     * Given a metric set, registers them.
     *
     * @param metrics a set of metrics
     * @throws IllegalArgumentException if any of the metrics are already registered
     */
    public void registerAll(MetricSet metrics) throws IllegalArgumentException {
        registerAll(null, metrics);
    }

    /**
     * Return the {@link Counter} registered under this group and name; or create and register
     * a new {@link Counter} if none is registered.
     *
     * @param group the group of the metric
     * @param name   the name of the metric
     * @return a new or pre-existing {@link Counter}
     */
    public Counter counter(String group, String name) {
        return getOrAdd(group, name, MetricBuilder.COUNTERS);
    }

    /**
     * Return the {@link Counter} registered under this group and name; or create and register
     * a new {@link Counter} using the provided MetricSupplier if none is registered.
     *
     * @param group   the group of the metric
     * @param name     the name of the metric
     * @param supplier a MetricSupplier that can be used to manufacture a counter.
     * @return a new or pre-existing {@link Counter}
     */
    public Counter counter(String group, String name, final MetricSupplier<Counter> supplier) {
        return getOrAdd(group, name, new MetricBuilder<Counter>() {
            @Override
            public Counter newMetric() {
                return supplier.newMetric();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Counter.class.isInstance(metric);
            }
        });
    }

    /**
     * Return the {@link Histogram} registered under this group and name; or create and register
     * a new {@link Histogram} if none is registered.
     *
     * @param group the group of the metric
     * @param name   the name of the metric
     * @return a new or pre-existing {@link Histogram}
     */
    public Histogram histogram(String group, String name) {
        return getOrAdd(group, name, MetricBuilder.HISTOGRAMS);
    }

    /**
     * Return the {@link Histogram} registered under this group and name; or create and register
     * a new {@link Histogram} using the provided MetricSupplier if none is registered.
     *
     * @param group   the group of the metric
     * @param name     the name of the metric
     * @param supplier a MetricSupplier that can be used to manufacture a histogram
     * @return a new or pre-existing {@link Histogram}
     */
    public Histogram histogram(String group, String name, final MetricSupplier<Histogram> supplier) {
        return getOrAdd(group, name, new MetricBuilder<Histogram>() {
            @Override
            public Histogram newMetric() {
                return supplier.newMetric();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Histogram.class.isInstance(metric);
            }
        });
    }

    /**
     * Return the {@link Meter} registered under this group and name; or create and register
     * a new {@link Meter} if none is registered.
     *
     * @param group the group of the metric
     * @param name   the name of the metric
     * @return a new or pre-existing {@link Meter}
     */
    public Meter meter(String group, String name) {
        return getOrAdd(group, name, MetricBuilder.METERS);
    }

    /**
     * Return the {@link Meter} registered under this group and name; or create and register
     * a new {@link Meter} using the provided MetricSupplier if none is registered.
     *
     * @param group   the group of the metric
     * @param name     the name of the metric
     * @param supplier a MetricSupplier that can be used to manufacture a Meter
     * @return a new or pre-existing {@link Meter}
     */
    public Meter meter(String group, String name, final MetricSupplier<Meter> supplier) {
        return getOrAdd(group, name, new MetricBuilder<Meter>() {
            @Override
            public Meter newMetric() {
                return supplier.newMetric();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Meter.class.isInstance(metric);
            }
        });
    }

    /**
     * Return the {@link Timer} registered under this group and name; or create and register
     * a new {@link Timer} if none is registered.
     *
     * @param group the group of the metric
     * @param name   the name of the metric
     * @return a new or pre-existing {@link Timer}
     */
    public Timer timer(String group, String name) {
        return getOrAdd(group, name, MetricBuilder.TIMERS);
    }

    /**
     * Return the {@link Timer} registered under this group and name; or create and register
     * a new {@link Timer} using the provided MetricSupplier if none is registered.
     *
     * @param group   the group of the metric
     * @param name     the name of the metric
     * @param supplier a MetricSupplier that can be used to manufacture a Timer
     * @return a new or pre-existing {@link Timer}
     */
    public Timer timer(String group, String name, final MetricSupplier<Timer> supplier) {
        return getOrAdd(group, name, new MetricBuilder<Timer>() {
            @Override
            public Timer newMetric() {
                return supplier.newMetric();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Timer.class.isInstance(metric);
            }
        });
    }

    /**
     * Return the {@link Gauge} registered under this group and name; or create and register
     * a new {@link Gauge} with provide gauge if none is registered.
     *
     * @param group the group of the metric
     * @param name   the name of the metric
     * @param gauge  the gauge that can be used to register
     * @return a new or pre-existing {@link Gauge}
     */
    public Gauge gauge(String group, String name, Gauge gauge) {
        return getOrAdd(group, name, Gauge.class, gauge);
    }

    /**
     * Return the {@link Gauge} registered under this group and name; or create and register
     * a new {@link Gauge} using the provided MetricSupplier if none is registered.
     *
     * @param group   the group of the metric
     * @param name     the name of the metric
     * @param supplier a MetricSupplier that can be used to manufacture a Gauge
     * @return a new or pre-existing {@link Gauge}
     */
    @SuppressWarnings("rawtypes")
    public Gauge gauge(String group, String name, final MetricSupplier<Gauge> supplier) {
        return getOrAdd(group, name, new MetricBuilder<Gauge>() {
            @Override
            public Gauge newMetric() {
                return supplier.newMetric();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Gauge.class.isInstance(metric);
            }
        });
    }

    /**
     * Removes the metric with the given group and name.
     *
     * @param group the group of the metric
     * @param name the name of the metric
     * @return whether or not the metric was removed
     */
    public boolean remove(String group, String name) {
        MetricId id = new MetricId(group, name);
        final Metric metric = metrics.remove(id);
        if (metric != null) {
            onMetricRemoved(id, metric);
            return true;
        }
        return false;
    }

    /**
     * Removes all metrics which match the given filter.
     *
     * @param filter a filter
     */
    public void removeMatching(MetricFilter filter) {
        for (Map.Entry<MetricId, Metric> entry : metrics.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                String group = entry.getKey().getGroup();
                String name = entry.getKey().getName();
                remove(group, name);
            }
        }
    }

    /**
     * Removes all registered metric. A helper method for unit test to reset metrics.
     */
    public void removeAllMetrics() {
        for (final MetricId id: metrics.keySet()) {
            String group = id.getGroup();
            String name = id.getName();
            remove(group, name);
        }
    }

    /**
     * Adds a {@link MetricRegistryListener} to a collection of listeners that will be notified on
     * metric creation.  Listeners will be notified in the order in which they are added.
     * <p>
     * <b>N.B.:</b> The listener will be notified of all existing metrics when it first registers.
     *
     * @param listener the listener that will be notified
     */
    public void addListener(MetricRegistryListener listener) {
        listeners.add(listener);

        for (Map.Entry<MetricId, Metric> entry : metrics.entrySet()) {
            notifyListenerOfAddedMetric(listener, entry.getValue(), entry.getKey());
        }
    }

    /**
     * Removes a {@link MetricRegistryListener} from this registry's collection of listeners.
     *
     * @param listener the listener that will be removed
     */
    public void removeListener(MetricRegistryListener listener) {
        listeners.remove(listener);
    }

    /**
     * Removes all {@link MetricRegistryListener} from this registry's collection of listeners.
     * A helper method for unit test to reset listeners.
     */
    public void removeAllListener() {
        listeners.clear();
    }

    /**
     * Returns a set of the ids of all the metrics in the registry.
     *
     * @return the ids of all the metrics
     */
    public Set<MetricId> getIds() {
        return Collections.unmodifiableSet(metrics.keySet());
    }

    /**
     * Returns a map of all the gauges in the registry and their ids.
     *
     * @return all the gauges in the registry
     */
    @SuppressWarnings("rawtypes")
    public SortedMap<MetricId, Gauge> getGauges() {
        return getGauges(MetricFilter.ALL);
    }

    /**
     * Returns a map of all the gauges in the registry and their ids which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the gauges in the registry
     */
    @SuppressWarnings("rawtypes")
    public SortedMap<MetricId, Gauge> getGauges(MetricFilter filter) {
        return getMetrics(Gauge.class, filter);
    }

    /**
     * Returns a map of all the counters in the registry and their ids.
     *
     * @return all the counters in the registry
     */
    public SortedMap<MetricId, Counter> getCounters() {
        return getCounters(MetricFilter.ALL);
    }

    /**
     * Returns a map of all the counters in the registry and their ids which match the given
     * filter.
     *
     * @param filter the metric filter to match
     * @return all the counters in the registry
     */
    public SortedMap<MetricId, Counter> getCounters(MetricFilter filter) {
        return getMetrics(Counter.class, filter);
    }

    /**
     * Returns a map of all the histograms in the registry and their ids.
     *
     * @return all the histograms in the registry
     */
    public SortedMap<MetricId, Histogram> getHistograms() {
        return getHistograms(MetricFilter.ALL);
    }

    /**
     * Returns a map of all the histograms in the registry and their ids which match the given
     * filter.
     *
     * @param filter the metric filter to match
     * @return all the histograms in the registry
     */
    public SortedMap<MetricId, Histogram> getHistograms(MetricFilter filter) {
        return getMetrics(Histogram.class, filter);
    }

    /**
     * Returns a map of all the meters in the registry and their ids.
     *
     * @return all the meters in the registry
     */
    public SortedMap<MetricId, Meter> getMeters() {
        return getMeters(MetricFilter.ALL);
    }

    /**
     * Returns a map of all the meters in the registry and their ids which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the meters in the registry
     */
    public SortedMap<MetricId, Meter> getMeters(MetricFilter filter) {
        return getMetrics(Meter.class, filter);
    }

    /**
     * Returns a map of all the timers in the registry and their ids.
     *
     * @return all the timers in the registry
     */
    public SortedMap<MetricId, Timer> getTimers() {
        return getTimers(MetricFilter.ALL);
    }

    /**
     * Returns a map of all the timers in the registry and their ids which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the timers in the registry
     */
    public SortedMap<MetricId, Timer> getTimers(MetricFilter filter) {
        return getMetrics(Timer.class, filter);
    }

    @SuppressWarnings("unchecked")
    private <T extends Metric> T getOrAdd(String group, String name, MetricBuilder<T> builder) {
        final MetricId id = new MetricId(group, name);
        final Metric metric = metrics.get(id);
        if (builder.isInstance(metric)) {
            return (T) metric;
        } else if (metric == null) {
            try {
                return register(group, name, builder.newMetric());
            } catch (IllegalArgumentException e) {
                final Metric added = metrics.get(id);
                if (builder.isInstance(added)) {
                    return (T) added;
                }
            }
        }
        throw new IllegalArgumentException(id + " is already used for a different type of metric");
    }

    @SuppressWarnings("unchecked")
    private <T extends Metric> T getOrAdd(String group, String name, Class<T> tClass, T metric) {
        final MetricId id = new MetricId(group, name);
        final Metric existMetric = metrics.get(id);
        if (tClass.isInstance(existMetric)) {
            return (T) existMetric;
        } else if (existMetric == null) {
            try {
                return register(group, name, metric);
            } catch (IllegalArgumentException e) {
                final Metric added = metrics.get(id);
                if (tClass.isInstance(added)) {
                    return (T) added;
                }
            }
        }
        throw new IllegalArgumentException(name + " is already used for a different type of metric");
    }

    @SuppressWarnings("unchecked")
    private <T extends Metric> SortedMap<MetricId, T> getMetrics(Class<T> klass, MetricFilter filter) {
        final TreeMap<MetricId, T> timers = new TreeMap<>();
        for (Map.Entry<MetricId, Metric> entry : metrics.entrySet()) {
            if (klass.isInstance(entry.getValue()) && filter.matches(entry.getKey(),
                    entry.getValue())) {
                timers.put(entry.getKey(), (T) entry.getValue());
            }
        }
        return Collections.unmodifiableSortedMap(timers);
    }

    private void onMetricAdded(MetricId id, Metric metric) {
        for (MetricRegistryListener listener : listeners) {
            notifyListenerOfAddedMetric(listener, metric, id);
        }
    }

    private void  notifyListenerOfAddedMetric(MetricRegistryListener listener, Metric metric, MetricId id) {
        if (metric instanceof Gauge) {
            listener.onGaugeAdded(id, (Gauge<?>) metric);
        } else if (metric instanceof Counter) {
            listener.onCounterAdded(id, (Counter) metric);
        } else if (metric instanceof Histogram) {
            listener.onHistogramAdded(id, (Histogram) metric);
        } else if (metric instanceof Meter) {
            listener.onMeterAdded(id, (Meter) metric);
        } else if (metric instanceof Timer) {
            listener.onTimerAdded(id, (Timer) metric);
        } else {
            throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
        }
    }

    private void onMetricRemoved(MetricId id, Metric metric) {
        for (MetricRegistryListener listener : listeners) {
            notifyListenerOfRemovedMetric(id, metric, listener);
        }
    }

    private void notifyListenerOfRemovedMetric(MetricId id, Metric metric, MetricRegistryListener listener) {
        if (metric instanceof Gauge) {
            listener.onGaugeRemoved(id);
        } else if (metric instanceof Counter) {
            listener.onCounterRemoved(id);
        } else if (metric instanceof Histogram) {
            listener.onHistogramRemoved(id);
        } else if (metric instanceof Meter) {
            listener.onMeterRemoved(id);
        } else if (metric instanceof Timer) {
            listener.onTimerRemoved(id);
        } else {
            throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
        }
    }

    private void registerAll(String prefix, MetricSet metrics) throws IllegalArgumentException {
        for (Map.Entry<MetricId, Metric> entry : metrics.getMetrics().entrySet()) {
            String group = entry.getKey().getGroup();
            String name = entry.getKey().getName();
            if (entry.getValue() instanceof MetricSet) {
                registerAll(name(prefix, group, name), (MetricSet) entry.getValue());
            } else {
                register(name(prefix, group), name, entry.getValue());
            }
        }
    }

    @Override
    public Map<MetricId, Metric> getMetrics() {
        return Collections.unmodifiableMap(metrics);
    }

    @FunctionalInterface
    public interface MetricSupplier<T extends Metric> {
        T newMetric();
    }

    /**
     * A quick and easy way of capturing the notion of default metrics.
     */
    private interface MetricBuilder<T extends Metric> {
        MetricBuilder<Counter> COUNTERS = new MetricBuilder<Counter>() {
            @Override
            public Counter newMetric() {
                return new Counter();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Counter.class.isInstance(metric);
            }
        };

        MetricBuilder<Histogram> HISTOGRAMS = new MetricBuilder<Histogram>() {
            @Override
            public Histogram newMetric() {
                return new Histogram(new ExponentiallyDecayingReservoir());
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Histogram.class.isInstance(metric);
            }
        };

        MetricBuilder<Meter> METERS = new MetricBuilder<Meter>() {
            @Override
            public Meter newMetric() {
                return new Meter();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Meter.class.isInstance(metric);
            }
        };

        MetricBuilder<Timer> TIMERS = new MetricBuilder<Timer>() {
            @Override
            public Timer newMetric() {
                return new Timer();
            }

            @Override
            public boolean isInstance(Metric metric) {
                return Timer.class.isInstance(metric);
            }
        };

        T newMetric();

        boolean isInstance(Metric metric);
    }
}
