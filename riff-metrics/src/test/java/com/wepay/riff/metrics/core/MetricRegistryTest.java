package com.wepay.riff.metrics.core;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.wepay.riff.metrics.core.MetricRegistry.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MetricRegistryTest {
    private final MetricRegistryListener listener = mock(MetricRegistryListener.class);
    private final MetricRegistry registry = MetricRegistry.getInstance();
    private final Gauge<String> gauge = () -> "";
    private final Counter counter = mock(Counter.class);
    private final Histogram histogram = mock(Histogram.class);
    private final Meter meter = mock(Meter.class);
    private final Timer timer = mock(Timer.class);

    @Before
    public void setUp() {
        registry.removeAllMetrics();
        registry.removeAllListener();
        registry.addListener(listener);
    }

    @Test
    public void registeringAGaugeTriggersANotification() {
        assertThat(registry.register("group", "thing", gauge))
                .isEqualTo(gauge);

        verify(listener).onGaugeAdded(new MetricId("group", "thing"), gauge);
    }

    @Test
    public void removingAGaugeTriggersANotification() {
        registry.register("group", "thing", gauge);

        assertThat(registry.remove("group", "thing"))
                .isTrue();

        verify(listener).onGaugeRemoved(new MetricId("group", "thing"));
    }

    @Test
    public void registeringACounterTriggersANotification() {
        assertThat(registry.register("group", "thing", counter))
                .isEqualTo(counter);

        verify(listener).onCounterAdded(new MetricId("group", "thing"), counter);
    }

    @Test
    public void accessingACounterRegistersAndReusesTheCounter() {
        final Counter counter1 = registry.counter("group", "thing");
        final Counter counter2 = registry.counter("group", "thing");

        assertThat(counter1)
                .isSameAs(counter2);

        verify(listener).onCounterAdded(new MetricId("group", "thing"), counter1);
    }

    @Test
    public void accessingACustomCounterRegistersAndReusesTheCounter() {
        final MetricRegistry.MetricSupplier<Counter> supplier = () -> counter;
        final Counter counter1 = registry.counter("group", "thing", supplier);
        final Counter counter2 = registry.counter("group", "thing", supplier);

        assertThat(counter1)
                .isSameAs(counter2);

        verify(listener).onCounterAdded(new MetricId("group", "thing"), counter1);
    }


    @Test
    public void removingACounterTriggersANotification() {
        registry.register("group", "thing", counter);

        assertThat(registry.remove("group", "thing"))
                .isTrue();

        verify(listener).onCounterRemoved(new MetricId("group", "thing"));
    }

    @Test
    public void registeringAHistogramTriggersANotification() {
        assertThat(registry.register("group", "thing", histogram))
                .isEqualTo(histogram);

        verify(listener).onHistogramAdded(new MetricId("group", "thing"), histogram);
    }

    @Test
    public void accessingAHistogramRegistersAndReusesIt() {
        final Histogram histogram1 = registry.histogram("group", "thing");
        final Histogram histogram2 = registry.histogram("group", "thing");

        assertThat(histogram1)
                .isSameAs(histogram2);

        verify(listener).onHistogramAdded(new MetricId("group", "thing"), histogram1);
    }

    @Test
    public void accessingACustomHistogramRegistersAndReusesIt() {
        final MetricRegistry.MetricSupplier<Histogram> supplier = () -> histogram;
        final Histogram histogram1 = registry.histogram("group", "thing", supplier);
        final Histogram histogram2 = registry.histogram("group", "thing", supplier);

        assertThat(histogram1)
                .isSameAs(histogram2);

        verify(listener).onHistogramAdded(new MetricId("group", "thing"), histogram1);
    }

    @Test
    public void removingAHistogramTriggersANotification() {
        registry.register("group", "thing", histogram);

        assertThat(registry.remove("group", "thing"))
                .isTrue();

        verify(listener).onHistogramRemoved(new MetricId("group", "thing"));
    }

    @Test
    public void registeringAMeterTriggersANotification() {
        assertThat(registry.register("group", "thing", meter))
                .isEqualTo(meter);

        verify(listener).onMeterAdded(new MetricId("group", "thing"), meter);
    }

    @Test
    public void accessingAMeterRegistersAndReusesIt() {
        final Meter meter1 = registry.meter("group", "thing");
        final Meter meter2 = registry.meter("group", "thing");

        assertThat(meter1)
                .isSameAs(meter2);

        verify(listener).onMeterAdded(new MetricId("group", "thing"), meter1);
    }

    @Test
    public void accessingACustomMeterRegistersAndReusesIt() {
        final MetricRegistry.MetricSupplier<Meter> supplier = () -> meter;
        final Meter meter1 = registry.meter("group", "thing", supplier);
        final Meter meter2 = registry.meter("group", "thing", supplier);

        assertThat(meter1)
                .isSameAs(meter2);

        verify(listener).onMeterAdded(new MetricId("group", "thing"), meter1);
    }

    @Test
    public void removingAMeterTriggersANotification() {
        registry.register("group", "thing", meter);

        assertThat(registry.remove("group", "thing"))
                .isTrue();

        verify(listener).onMeterRemoved(new MetricId("group", "thing"));
    }

    @Test
    public void registeringATimerTriggersANotification() {
        assertThat(registry.register("group", "thing", timer))
                .isEqualTo(timer);

        verify(listener).onTimerAdded(new MetricId("group", "thing"), timer);
    }

    @Test
    public void accessingATimerRegistersAndReusesIt() {
        final Timer timer1 = registry.timer("group", "thing");
        final Timer timer2 = registry.timer("group", "thing");

        assertThat(timer1)
                .isSameAs(timer2);

        verify(listener).onTimerAdded(new MetricId("group", "thing"), timer1);
    }

    @Test
    public void accessingACustomTimerRegistersAndReusesIt() {
        final MetricRegistry.MetricSupplier<Timer> supplier = () -> timer;
        final Timer timer1 = registry.timer("group", "thing", supplier);
        final Timer timer2 = registry.timer("group", "thing", supplier);

        assertThat(timer1)
                .isSameAs(timer2);

        verify(listener).onTimerAdded(new MetricId("group", "thing"), timer1);
    }


    @Test
    public void removingATimerTriggersANotification() {
        registry.register("group", "thing", timer);

        assertThat(registry.remove("group", "thing"))
                .isTrue();

        verify(listener).onTimerRemoved(new MetricId("group", "thing"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void accessingACustomGaugeRegistersAndReusesIt() {
        final MetricRegistry.MetricSupplier<Gauge> supplier = () -> gauge;
        final Gauge gauge1 = registry.gauge("group", "thing", supplier);
        final Gauge gauge2 = registry.gauge("group", "thing", supplier);

        assertThat(gauge1)
                .isSameAs(gauge2);

        verify(listener).onGaugeAdded(new MetricId("group", "thing"), gauge1);
    }


    @Test
    public void addingAListenerWithExistingMetricsCatchesItUp() {
        registry.register("group", "gauge", gauge);
        registry.register("group", "counter", counter);
        registry.register("group", "histogram", histogram);
        registry.register("group", "meter", meter);
        registry.register("group", "timer", timer);

        final MetricRegistryListener other = mock(MetricRegistryListener.class);
        registry.addListener(other);

        verify(other).onGaugeAdded(new MetricId("group", "gauge"), gauge);
        verify(other).onCounterAdded(new MetricId("group", "counter"), counter);
        verify(other).onHistogramAdded(new MetricId("group", "histogram"), histogram);
        verify(other).onMeterAdded(new MetricId("group", "meter"), meter);
        verify(other).onTimerAdded(new MetricId("group", "timer"), timer);
    }

    @Test
    public void aRemovedListenerDoesNotReceiveUpdates() {
        registry.register("group", "gauge", gauge);
        registry.removeListener(listener);
        registry.register("group", "gauge2", gauge);

        verify(listener, never()).onGaugeAdded(new MetricId("group", "gauge2"), gauge);
    }

    @Test
    public void hasAMapOfRegisteredGauges() {
        registry.register("group", "gauge", gauge);

        assertThat(registry.getGauges())
                .contains(entry(new MetricId("group", "gauge"), gauge));
    }

    @Test
    public void hasAMapOfRegisteredCounters() {
        registry.register("group", "counter", counter);

        assertThat(registry.getCounters())
                .contains(entry(new MetricId("group", "counter"), counter));
    }

    @Test
    public void hasAMapOfRegisteredHistograms() {
        registry.register("group", "histogram", histogram);

        assertThat(registry.getHistograms())
                .contains(entry(new MetricId("group", "histogram"), histogram));
    }

    @Test
    public void hasAMapOfRegisteredMeters() {
        registry.register("group", "meter", meter);

        assertThat(registry.getMeters())
                .contains(entry(new MetricId("group", "meter"), meter));
    }

    @Test
    public void hasAMapOfRegisteredTimers() {
        registry.register("group", "timer", timer);

        assertThat(registry.getTimers())
                .contains(entry(new MetricId("group", "timer"), timer));
    }

    @Test
    public void hasASetOfRegisteredMetricNames() {
        registry.register("group", "gauge", gauge);
        registry.register("group", "counter", counter);
        registry.register("group", "histogram", histogram);
        registry.register("group", "meter", meter);
        registry.register("group", "timer", timer);

        assertThat(registry.getIds().stream().map(id -> id.getFullName()).collect(Collectors.toSet()))
                .containsOnly("group.gauge", "group.counter", "group.histogram", "group.meter", "group.timer");
    }

    @Test
    public void registersMultipleMetrics() {
        final MetricSet metrics = () -> {
            final Map<MetricId, Metric> m = new HashMap<>();
            m.put(new MetricId("group", "gauge"), gauge);
            m.put(new MetricId("group", "counter"), counter);
            return m;
        };

        registry.registerAll(metrics);

        assertThat(registry.getIds().stream().map(id -> id.getFullName()).collect(Collectors.toSet()))
                .containsOnly("group.gauge", "group.counter");
    }

    @Test
    public void registersMultipleMetricsWithAPrefix() {
        final MetricSet metrics = () -> {
            final Map<MetricId, Metric> m = new HashMap<>();
            m.put(new MetricId("", "gauge"), gauge);
            m.put(new MetricId("", "counter"), counter);
            return m;
        };

        registry.register("group", "my", metrics);

        assertThat(registry.getIds().stream().map(id -> id.getFullName()).collect(Collectors.toSet()))
                .containsOnly("group.my.gauge", "group.my.counter");
    }

    @Test
    public void registersRecursiveMetricSets() {
        final MetricSet inner = () -> {
            final Map<MetricId, Metric> m = new HashMap<>();
            m.put(new MetricId("", "gauge"), gauge);
            return m;
        };

        final MetricSet outer = () -> {
            final Map<MetricId, Metric> m = new HashMap<>();
            m.put(new MetricId("", "inner"), inner);
            m.put(new MetricId("", "counter"), counter);
            return m;
        };

        registry.register("group", "my", outer);

        assertThat(registry.getIds().stream().map(id -> id.getFullName()).collect(Collectors.toSet()))
                .containsOnly("group.my.inner.gauge", "group.my.counter");
    }

    @Test
    public void concatenatesStringsToFormADottedName() {
        assertThat(name("one", "two", "three"))
                .isEqualTo("one.two.three");
    }

    @Test
    @SuppressWarnings("NullArgumentToVariableArgMethod")
    public void elidesNullValuesFromNamesWhenOnlyOneNullPassedIn() {
        assertThat(name("one", (String) null))
                .isEqualTo("one");
    }

    @Test
    public void elidesNullValuesFromNamesWhenManyNullsPassedIn() {
        assertThat(name("one", null, null))
                .isEqualTo("one");
    }

    @Test
    public void elidesNullValuesFromNamesWhenNullAndNotNullPassedIn() {
        assertThat(name("one", null, "three"))
                .isEqualTo("one.three");
    }

    @Test
    public void elidesEmptyStringsFromNames() {
        assertThat(name("one", "", "three"))
                .isEqualTo("one.three");
    }

    @Test
    public void concatenatesClassNamesWithStringsToFormADottedName() {
        assertThat(name(MetricRegistryTest.class, "one", "two"))
                .isEqualTo("com.wepay.riff.metrics.core.MetricRegistryTest.one.two");
    }

    @Test
    public void concatenatesClassesWithoutCanonicalNamesWithStrings() {
        final Gauge<String> g = () -> null;

        assertThat(name(g.getClass(), "one", "two"))
                .matches("com.wepay.riff.metrics.core.MetricRegistryTest.+?\\.one\\.two");
    }

    @Test
    public void removesMetricsMatchingAFilter() {
        registry.timer("group", "timer-1");
        registry.timer("group", "timer-2");
        registry.histogram("group", "histogram-1");

        assertThat(registry.getIds().stream().map(id -> id.getName()).collect(Collectors.toSet()))
                .contains("timer-1", "timer-2", "histogram-1");

        registry.removeMatching((name, metric) -> name.getFullName().endsWith("1"));

        assertThat(registry.getIds().stream().map(id -> id.getName()).collect(Collectors.toSet()))
                .doesNotContain("timer-1", "histogram-1");
        assertThat(registry.getIds().stream().map(id -> id.getName()).collect(Collectors.toSet()))
                .contains("timer-2");

        verify(listener).onTimerRemoved(new MetricId("group", "timer-1"));
        verify(listener).onHistogramRemoved(new MetricId("group", "histogram-1"));
    }
}
