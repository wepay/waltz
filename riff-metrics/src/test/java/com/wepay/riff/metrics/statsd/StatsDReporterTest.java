package com.wepay.riff.metrics.statsd;

import com.wepay.riff.metrics.core.Counter;
import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.Histogram;
import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.metrics.core.MetricFilter;
import com.wepay.riff.metrics.core.MetricId;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.core.Snapshot;
import com.wepay.riff.metrics.core.Timer;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatsDReporterTest {
  private final StatsD statsD = mock(StatsD.class);
  private final MetricRegistry registry = mock(MetricRegistry.class);
  private final StatsDReporter reporter = StatsDReporter.forRegistry(registry)
      .prefixedWith("prefix")
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .filter(MetricFilter.ALL)
      .build(statsD);

  @SuppressWarnings("rawtypes") //Metrics library specifies the raw Gauge type unfortunately
  private final SortedMap<MetricId, Gauge> emptyGaugeMap = new TreeMap<>();

  @Test
  public void doesNotReportStringGaugeValues() throws Exception {
    reporter.report(map(new MetricId("group", "gauge"), gauge("value")), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD, never()).send("prefix.gauge", "value");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsByteGaugeValues() throws Exception {
    reporter.report(map(new MetricId("group", "gauge"), gauge((byte) 1)), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD).send("prefix.group.gauge", "1");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsShortGaugeValues() throws Exception {
    reporter.report(map(new MetricId("group", "gauge"), gauge((short) 1)), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD).send("prefix.group.gauge", "1");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsIntegerGaugeValues() throws Exception {
    reporter.report(map(new MetricId("group", "gauge"), gauge(1)), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD).send("prefix.group.gauge", "1");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsLongGaugeValues() throws Exception {
    reporter.report(map(new MetricId("group", "gauge"), gauge(1L)), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD).send("prefix.group.gauge", "1");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsFloatGaugeValues() throws Exception {
    reporter.report(map(new MetricId("group", "gauge"), gauge(1.1f)), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD).send("prefix.group.gauge", "1.10");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsDoubleGaugeValues() throws Exception {
    reporter.report(map(new MetricId("group", "gauge"), gauge(1.1)), this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD).send("prefix.group.gauge", "1.10");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsCounters() throws Exception {
    final Counter counter = mock(Counter.class);
    when(counter.getCount()).thenReturn(100L);

    reporter.report(emptyGaugeMap, this.<Counter>map(new MetricId("group", "counter"), counter), this.<Histogram>map(),
        this.<Meter>map(), this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    inOrder.verify(statsD).send("prefix.group.counter", "100");
    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsHistograms() throws Exception {
    final Histogram histogram = mock(Histogram.class);
    when(histogram.getCount()).thenReturn(1L);

    final Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.getMax()).thenReturn(2L);
    when(snapshot.getMean()).thenReturn(3.0);
    when(snapshot.getMin()).thenReturn(4L);
    when(snapshot.getStdDev()).thenReturn(5.0);
    when(snapshot.getMedian()).thenReturn(6.0);
    when(snapshot.get75thPercentile()).thenReturn(7.0);
    when(snapshot.get95thPercentile()).thenReturn(8.0);
    when(snapshot.get98thPercentile()).thenReturn(9.0);
    when(snapshot.get99thPercentile()).thenReturn(10.0);
    when(snapshot.get999thPercentile()).thenReturn(11.0);

    when(histogram.getSnapshot()).thenReturn(snapshot);

    reporter.report(emptyGaugeMap, this.<Counter>map(), this.<Histogram>map(new MetricId("group", "histogram"), histogram),
        this.<Meter>map(), this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);

    inOrder.verify(statsD).connect();
    verify(statsD).send("prefix.group.histogram.samples", "1");
    verify(statsD).send("prefix.group.histogram.max", "2");
    verify(statsD).send("prefix.group.histogram.mean", "3.00");
    verify(statsD).send("prefix.group.histogram.min", "4");
    verify(statsD).send("prefix.group.histogram.stddev", "5.00");
    verify(statsD).send("prefix.group.histogram.p50", "6.00");
    verify(statsD).send("prefix.group.histogram.p75", "7.00");
    verify(statsD).send("prefix.group.histogram.p95", "8.00");
    verify(statsD).send("prefix.group.histogram.p98", "9.00");
    verify(statsD).send("prefix.group.histogram.p99", "10.00");
    inOrder.verify(statsD).send("prefix.group.histogram.p999", "11.00");

    inOrder.verify(statsD).close();
  }

  @Test
  public void reportsMeters() throws Exception {
    final Meter meter = mock(Meter.class);
    when(meter.getCount()).thenReturn(1L);
    when(meter.getOneMinuteRate()).thenReturn(2.0);
    when(meter.getFiveMinuteRate()).thenReturn(3.0);
    when(meter.getFifteenMinuteRate()).thenReturn(4.0);
    when(meter.getMeanRate()).thenReturn(5.0);

    reporter.report(emptyGaugeMap, this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(new MetricId("group", "meter"), meter),
        this.<Timer>map());

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    verify(statsD).send("prefix.group.meter.samples", "1");
    verify(statsD).send("prefix.group.meter.m1_rate", "2.00");
    verify(statsD).send("prefix.group.meter.m5_rate", "3.00");
    verify(statsD).send("prefix.group.meter.m15_rate", "4.00");
    inOrder.verify(statsD).send("prefix.group.meter.mean_rate", "5.00");
    inOrder.verify(statsD).close();


  }

  @Test
  public void reportsTimers() throws Exception {
    final Timer timer = mock(Timer.class);
    when(timer.getCount()).thenReturn(1L);
    when(timer.getMeanRate()).thenReturn(2.0);
    when(timer.getOneMinuteRate()).thenReturn(3.0);
    when(timer.getFiveMinuteRate()).thenReturn(4.0);
    when(timer.getFifteenMinuteRate()).thenReturn(5.0);

    final Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.getMax()).thenReturn(TimeUnit.MILLISECONDS.toNanos(100));
    when(snapshot.getMean()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(200));
    when(snapshot.getMin()).thenReturn(TimeUnit.MILLISECONDS.toNanos(300));
    when(snapshot.getStdDev()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(400));
    when(snapshot.getMedian()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(500));
    when(snapshot.get75thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(600));
    when(snapshot.get95thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(700));
    when(snapshot.get98thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(800));
    when(snapshot.get99thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(900));
    when(snapshot.get999thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(1000));

    when(timer.getSnapshot()).thenReturn(snapshot);

    reporter.report(emptyGaugeMap, this.<Counter>map(), this.<Histogram>map(), this.<Meter>map(),
        map(new MetricId("group", "timer"), timer));

    final InOrder inOrder = inOrder(statsD);
    inOrder.verify(statsD).connect();
    verify(statsD).send("prefix.group.timer.max", "100.00");
    verify(statsD).send("prefix.group.timer.mean", "200.00");
    verify(statsD).send("prefix.group.timer.min", "300.00");
    verify(statsD).send("prefix.group.timer.stddev", "400.00");
    verify(statsD).send("prefix.group.timer.p50", "500.00");
    verify(statsD).send("prefix.group.timer.p75", "600.00");
    verify(statsD).send("prefix.group.timer.p95", "700.00");
    verify(statsD).send("prefix.group.timer.p98", "800.00");
    verify(statsD).send("prefix.group.timer.p99", "900.00");
    verify(statsD).send("prefix.group.timer.p999", "1000.00");
    verify(statsD).send("prefix.group.timer.samples", "1");
    verify(statsD).send("prefix.group.timer.m1_rate", "3.00");
    verify(statsD).send("prefix.group.timer.m5_rate", "4.00");
    verify(statsD).send("prefix.group.timer.m15_rate", "5.00");
    inOrder.verify(statsD).send("prefix.group.timer.mean_rate", "2.00");
    inOrder.verify(statsD).close();
  }

  private <T> SortedMap<MetricId, T> map() {
    return new TreeMap<>();
  }

  private <T> SortedMap<MetricId, T> map(MetricId id, T metric) {
    final TreeMap<MetricId, T> map = new TreeMap<>();
    map.put(id, metric);
    return map;
  }

  @SuppressWarnings("rawtypes")
  private <T> Gauge gauge(T value) {
    final Gauge gauge = mock(Gauge.class);
    when(gauge.getValue()).thenReturn(value);
    return gauge;
  }
}
