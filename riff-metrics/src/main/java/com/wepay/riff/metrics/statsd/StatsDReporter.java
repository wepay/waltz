package com.wepay.riff.metrics.statsd;

import com.wepay.riff.metrics.core.Counter;
import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.Histogram;
import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.metrics.core.Metered;
import com.wepay.riff.metrics.core.MetricFilter;
import com.wepay.riff.metrics.core.MetricId;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.core.Snapshot;
import com.wepay.riff.metrics.core.Timer;
import com.wepay.riff.metrics.core.ScheduledReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a StatsD server.
 *
 * @see <a href="https://github.com/etsy/statsd">StatsD</a>
 */
@NotThreadSafe
public final class StatsDReporter extends ScheduledReporter {
  private static final Logger LOG = LoggerFactory.getLogger(StatsDReporter.class);

  private final StatsD statsD;
  private final String prefix;

  private StatsDReporter(final MetricRegistry registry,
                         final StatsD statsD,
                         final String prefix,
                         final TimeUnit rateUnit,
                         final TimeUnit durationUnit,
                         final MetricFilter filter) {
    super(registry, "statsd-reporter", filter, rateUnit, durationUnit);
    this.statsD = statsD;
    this.prefix = prefix;
  }

  /**
   * Returns a new {@link Builder} for {@link StatsDReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link StatsDReporter}
   */
  public static Builder forRegistry(final MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link StatsDReporter} instances. Defaults to not using a prefix,
   * converting rates to events/second, converting durations to milliseconds, and not
   * filtering metrics.
   */
  @NotThreadSafe
  public static final class Builder {
    private final MetricRegistry registry;
    private String prefix;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(final MetricRegistry registry) {
      this.registry = registry;
      this.prefix = null;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    /**
     * Prefix all metric names with the given string.
     *
     * @param prefix the prefix for all metric names
     * @return {@code this}
     */
    public Builder prefixedWith(@Nullable final String prefix) {
      this.prefix = prefix;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(final TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(final TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(final MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Builds a {@link StatsDReporter} with the given properties, sending metrics to StatsD at the given host and port.
     *
     * @param host the hostname of the StatsD server.
     * @param port the port of the StatsD server. This is typically 8125.
     * @return a {@link StatsDReporter}
     */
    public StatsDReporter build(final String host, final int port) {
      return build(new StatsD(host, port));
    }

    /**
     * Builds a {@link StatsDReporter} with the given properties, sending metrics using the
     * given {@link StatsD} client.
     *
     * @param statsD a {@link StatsD} client
     * @return a {@link StatsDReporter}
     */
    public StatsDReporter build(final StatsD statsD) {
      return new StatsDReporter(registry, statsD, prefix, rateUnit, durationUnit, filter);
    }
  }


  @Override
  @SuppressWarnings("rawtypes") //Metrics 3.0 interface specifies the raw Gauge type
  public void report(final SortedMap<MetricId, Gauge> gauges,
                     final SortedMap<MetricId, Counter> counters,
                     final SortedMap<MetricId, Histogram> histograms,
                     final SortedMap<MetricId, Meter> meters,
                     final SortedMap<MetricId, Timer> timers) {

    try {
      statsD.connect();

      for (Map.Entry<MetricId, Gauge> entry : gauges.entrySet()) {
        reportGauge(entry.getKey().getFullName(), entry.getValue());
      }

      for (Map.Entry<MetricId, Counter> entry : counters.entrySet()) {
        reportCounter(entry.getKey().getFullName(), entry.getValue());
      }

      for (Map.Entry<MetricId, Histogram> entry : histograms.entrySet()) {
        reportHistogram(entry.getKey().getFullName(), entry.getValue());
      }

      for (Map.Entry<MetricId, Meter> entry : meters.entrySet()) {
        reportMetered(entry.getKey().getFullName(), entry.getValue());
      }

      for (Map.Entry<MetricId, Timer> entry : timers.entrySet()) {
        reportTimer(entry.getKey().getFullName(), entry.getValue());
      }
    } catch (IOException e) {
      LOG.warn("Unable to report to StatsD", statsD, e);
    } finally {
      try {
        statsD.close();
      } catch (IOException e) {
        LOG.debug("Error disconnecting from StatsD", statsD, e);
      }
    }
  }

  private void reportTimer(final String name, final Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();

    statsD.send(prefix(name, "max"), formatNumber(convertDuration(snapshot.getMax())));
    statsD.send(prefix(name, "mean"), formatNumber(convertDuration(snapshot.getMean())));
    statsD.send(prefix(name, "min"), formatNumber(convertDuration(snapshot.getMin())));
    statsD.send(prefix(name, "stddev"), formatNumber(convertDuration(snapshot.getStdDev())));
    statsD.send(prefix(name, "p50"), formatNumber(convertDuration(snapshot.getMedian())));
    statsD.send(prefix(name, "p75"), formatNumber(convertDuration(snapshot.get75thPercentile())));
    statsD.send(prefix(name, "p95"), formatNumber(convertDuration(snapshot.get95thPercentile())));
    statsD.send(prefix(name, "p98"), formatNumber(convertDuration(snapshot.get98thPercentile())));
    statsD.send(prefix(name, "p99"), formatNumber(convertDuration(snapshot.get99thPercentile())));
    statsD.send(prefix(name, "p999"), formatNumber(convertDuration(snapshot.get999thPercentile())));

    reportMetered(name, timer);
  }

  private void reportMetered(final String name, final Metered meter) {
    statsD.send(prefix(name, "samples"), formatNumber(meter.getCount()));
    statsD.send(prefix(name, "m1_rate"), formatNumber(convertRate(meter.getOneMinuteRate())));
    statsD.send(prefix(name, "m5_rate"), formatNumber(convertRate(meter.getFiveMinuteRate())));
    statsD.send(prefix(name, "m15_rate"), formatNumber(convertRate(meter.getFifteenMinuteRate())));
    statsD.send(prefix(name, "mean_rate"), formatNumber(convertRate(meter.getMeanRate())));
  }

  private void reportHistogram(final String name, final Histogram histogram) {
    final Snapshot snapshot = histogram.getSnapshot();
    statsD.send(prefix(name, "samples"), formatNumber(histogram.getCount()));
    statsD.send(prefix(name, "max"), formatNumber(snapshot.getMax()));
    statsD.send(prefix(name, "mean"), formatNumber(snapshot.getMean()));
    statsD.send(prefix(name, "min"), formatNumber(snapshot.getMin()));
    statsD.send(prefix(name, "stddev"), formatNumber(snapshot.getStdDev()));
    statsD.send(prefix(name, "p50"), formatNumber(snapshot.getMedian()));
    statsD.send(prefix(name, "p75"), formatNumber(snapshot.get75thPercentile()));
    statsD.send(prefix(name, "p95"), formatNumber(snapshot.get95thPercentile()));
    statsD.send(prefix(name, "p98"), formatNumber(snapshot.get98thPercentile()));
    statsD.send(prefix(name, "p99"), formatNumber(snapshot.get99thPercentile()));
    statsD.send(prefix(name, "p999"), formatNumber(snapshot.get999thPercentile()));
  }

  private void reportCounter(final String name, final Counter counter) {
    statsD.send(prefix(name), formatNumber(counter.getCount()));
  }

  @SuppressWarnings("rawtypes") //Metrics 3.0 passes us the raw Gauge type
  private void reportGauge(final String name, final Gauge gauge) {
    final String value = format(gauge.getValue());
    if (value != null) {
      statsD.send(prefix(name), value);
    }
  }

  @Nullable
  private String format(final Object o) {
    if (o instanceof Float) {
      return formatNumber(((Float) o).doubleValue());
    } else if (o instanceof Double) {
      return formatNumber((Double) o);
    } else if (o instanceof Byte) {
      return formatNumber(((Byte) o).longValue());
    } else if (o instanceof Short) {
      return formatNumber(((Short) o).longValue());
    } else if (o instanceof Integer) {
      return formatNumber(((Integer) o).longValue());
    } else if (o instanceof Long) {
      return formatNumber((Long) o);
    } else if (o instanceof BigInteger) {
      return formatNumber((BigInteger) o);
    } else if (o instanceof BigDecimal) {
      return formatNumber(((BigDecimal) o).doubleValue());
    }
    return null;
  }

  private String prefix(final String... components) {
    return MetricRegistry.name(prefix, components);
  }

  private String formatNumber(final BigInteger n) {
    return String.valueOf(n);
  }

  private String formatNumber(final long n) {
    return Long.toString(n);
  }

  private String formatNumber(final double v) {
    return String.format(Locale.US, "%2.2f", v);
  }
}
