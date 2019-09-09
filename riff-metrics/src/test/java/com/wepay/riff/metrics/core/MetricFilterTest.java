package com.wepay.riff.metrics.core;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class MetricFilterTest {
    @Test
    public void theAllFilterMatchesAllMetrics() {
        assertThat(MetricFilter.ALL.matches(new MetricId("", ""), mock(Metric.class)))
                .isTrue();
    }

    @Test
    public void theStartsWithFilterMatches() {
        assertThat(MetricFilter.startsWith("foo").matches(new MetricId("foo", "bar"), mock(Metric.class)))
                .isTrue();
        assertThat(MetricFilter.startsWith("foo").matches(new MetricId("bar", "foo"), mock(Metric.class)))
                .isFalse();
    }

    @Test
    public void theEndsWithFilterMatches() {
        assertThat(MetricFilter.endsWith("foo").matches(new MetricId("foo", "bar"), mock(Metric.class)))
                .isFalse();
        assertThat(MetricFilter.endsWith("foo").matches(new MetricId("bar", "foo"), mock(Metric.class)))
                .isTrue();
    }

    @Test
    public void theContainsFilterMatches() {
        assertThat(MetricFilter.contains("foo").matches(new MetricId("bar.foo", "bar"), mock(Metric.class)))
                .isTrue();
        assertThat(MetricFilter.contains("foo").matches(new MetricId("bar", "bar"), mock(Metric.class)))
                .isFalse();
    }
}
