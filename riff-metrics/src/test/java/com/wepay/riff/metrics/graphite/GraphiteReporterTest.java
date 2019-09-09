package com.wepay.riff.metrics.graphite;

import com.wepay.riff.metrics.core.Clock;
import com.wepay.riff.metrics.core.Counter;
import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.Histogram;
import com.wepay.riff.metrics.core.Meter;
import com.wepay.riff.metrics.core.MetricFilter;
import com.wepay.riff.metrics.core.MetricId;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.core.Snapshot;
import com.wepay.riff.metrics.core.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class GraphiteReporterTest {

    private static final long TIMESTAMP = 9999999;
    private final Graphite graphite = mock(Graphite.class);
    private final MetricRegistry metricRegistry = mock(MetricRegistry.class);
    private final Clock clock = mock(Clock.class);
    private final GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
            .withClock(clock)
            .prefixedWith("prefix")
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .disabledMetricAttributes(Collections.emptySet())
            .build(graphite);

    @Before
    public void setUp() throws Exception {
        when(clock.getTime()).thenReturn(TIMESTAMP * 1000);
    }

    @Test
    public void doesNotReportStringGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge("value")),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite, never()).send("prefix.group.gauge", "value", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsByteGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge((byte) 1)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsShortGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge((short) 1)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsIntegerGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge(1)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsLongGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge(1L)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsFloatGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge(1.1f)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1.10", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsDoubleGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge(1.1)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1.10", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsDoubleGaugeValuesWithFormatting() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge(1.1322)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1.13", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsBigIntegerGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge(new BigInteger("1"))),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1.00", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsBigDecimalGaugeValues() throws Exception {
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge(new BigDecimal("1"))),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.gauge", "1.00", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsCounters() throws Exception {
        final Counter counter = mock(Counter.class);
        when(counter.getCount()).thenReturn(10L);

        graphiteReporter.report(TestHelper.map(),
                TestHelper.map(new MetricId("group", "counter"), counter),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = Mockito.inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.counter.count", "10", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
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

        graphiteReporter.report(TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(new MetricId("group", "histogram"), histogram),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = inOrder(graphite);

        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).send("prefix.group.histogram.count", "1", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.max", "2", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.mean", "3.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.min", "4", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.stddev", "5.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.p50", "6.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.p75", "7.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.p95", "8.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.p98", "9.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.p99", "10.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.histogram.p999", "11.00", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void reportsMeters() throws Exception {
        final Meter meter = mock(Meter.class);
        when(meter.getCount()).thenReturn(1L);
        when(meter.getOneMinuteRate()).thenReturn(2.0);
        when(meter.getFiveMinuteRate()).thenReturn(3.0);
        when(meter.getFifteenMinuteRate()).thenReturn(4.0);
        when(meter.getMeanRate()).thenReturn(5.0);

        graphiteReporter.report(TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(new MetricId("group", "meter"), meter),
                TestHelper.map());

        final InOrder inOrder = inOrder(graphite);
        inOrder.verify(graphite).connect();
        verify(graphite).send("prefix.group.meter.count", "1", TIMESTAMP);
        verify(graphite).send("prefix.group.meter.m1_rate", "2.00", TIMESTAMP);
        verify(graphite).send("prefix.group.meter.m5_rate", "3.00", TIMESTAMP);
        verify(graphite).send("prefix.group.meter.m15_rate", "4.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.meter.mean_rate", "5.00", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
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

        graphiteReporter.report(TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(new MetricId("group", "timer"), timer));

        final InOrder inOrder = inOrder(graphite);
        inOrder.verify(graphite).connect();
        verify(graphite).send("prefix.group.timer.max", "100.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.mean", "200.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.min", "300.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.stddev", "400.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.p50", "500.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.p75", "600.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.p95", "700.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.p98", "800.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.p99", "900.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.p999", "1000.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.count", "1", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.m1_rate", "3.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.m5_rate", "4.00", TIMESTAMP);
        verify(graphite).send("prefix.group.timer.m15_rate", "5.00", TIMESTAMP);
        inOrder.verify(graphite).send("prefix.group.timer.mean_rate", "2.00", TIMESTAMP);
        inOrder.verify(graphite).flush();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void testIncorrectGraphiteServerHostname() throws Exception {
        doThrow(new UnknownHostException("Unknown Host")).when(graphite).connect();
        graphiteReporter.report(TestHelper.map(new MetricId("group", "gauge"), TestHelper.gauge((byte) 1)),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map(),
                TestHelper.map());

        final InOrder inOrder = inOrder(graphite);
        inOrder.verify(graphite).connect();
        inOrder.verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    @Test
    public void testGraphiteSenderCloseOnReporterClose() throws Exception {
        graphiteReporter.stop();

        verify(graphite).close();

        verifyNoMoreInteractions(graphite);
    }

    private static class TestHelper {

        private static <T> SortedMap<MetricId, T> map() {
            return new TreeMap<>();
        }

        private static <T> SortedMap<MetricId, T> map(MetricId id, T metric) {
            final TreeMap<MetricId, T> map = new TreeMap<>();
            map.put(id, metric);
            return map;
        }

        private static <T> Gauge gauge(T value) {
            final Gauge gauge = mock(Gauge.class);
            when(gauge.getValue()).thenReturn(value);
            return gauge;
        }
    }
}
