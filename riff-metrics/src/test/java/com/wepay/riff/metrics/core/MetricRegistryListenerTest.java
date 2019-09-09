package com.wepay.riff.metrics.core;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class MetricRegistryListenerTest {
    private final Counter counter = mock(Counter.class);
    private final Histogram histogram = mock(Histogram.class);
    private final Meter meter = mock(Meter.class);
    private final Timer timer = mock(Timer.class);
    private final MetricRegistryListener listener = new MetricRegistryListener.Base() {

    };

    @Test
    public void noOpsOnGaugeAdded() {
        listener.onGaugeAdded(new MetricId("group", "blah"), () -> {
            throw new RuntimeException("Should not be called");
        });
    }

    @Test
    public void noOpsOnCounterAdded() {
        listener.onCounterAdded(new MetricId("group", "blah"), counter);

        verifyZeroInteractions(counter);
    }

    @Test
    public void noOpsOnHistogramAdded() {
        listener.onHistogramAdded(new MetricId("group", "blah"), histogram);

        verifyZeroInteractions(histogram);
    }

    @Test
    public void noOpsOnMeterAdded() {
        listener.onMeterAdded(new MetricId("group", "blah"), meter);

        verifyZeroInteractions(meter);
    }

    @Test
    public void noOpsOnTimerAdded() {
        listener.onTimerAdded(new MetricId("group", "blah"), timer);

        verifyZeroInteractions(timer);
    }

    @Test
    public void doesNotExplodeWhenMetricsAreRemoved() {
        listener.onGaugeRemoved(new MetricId("group", "blah"));
        listener.onCounterRemoved(new MetricId("group", "blah"));
        listener.onHistogramRemoved(new MetricId("group", "blah"));
        listener.onMeterRemoved(new MetricId("group", "blah"));
        listener.onTimerRemoved(new MetricId("group", "blah"));
    }
}
