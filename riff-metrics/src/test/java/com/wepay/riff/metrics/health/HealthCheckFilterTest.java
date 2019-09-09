package com.wepay.riff.metrics.health;


import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class HealthCheckFilterTest {

    @Test
    public void theAllFilterMatchesAllHealthChecks() {
        assertThat(HealthCheckFilter.ALL.matches("", mock(HealthCheck.class))).isTrue();
    }
}
