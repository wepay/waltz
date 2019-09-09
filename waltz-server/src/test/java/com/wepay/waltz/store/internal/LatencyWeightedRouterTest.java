package com.wepay.waltz.store.internal;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LatencyWeightedRouterTest {

    private static final int REPEAT = 1000;
    private static final int NUM_ROUTES = 3;

    private LatencyWeightedRouter<MockLatencyWeightedRoute> router;
    private List<MockLatencyWeightedRoute> routes;

    @Before
    public void setup() {
        routes = Arrays.asList(new MockLatencyWeightedRoute(), new MockLatencyWeightedRoute(), new MockLatencyWeightedRoute());
        router = new LatencyWeightedRouter<>(routes);
    }

    @Test
    public void testEvenRouting() {
        MockLatencyWeightedRoute lastRoute = null;

        for (int i = 0; i < REPEAT; i++) {
            MockLatencyWeightedRoute route = router.getRoute();
            assertNotEquals(lastRoute, route);

            if (route != null) {
                route.incr();
                route.updateExpectedLatency(10);
                lastRoute = route;

            } else {
                fail();
            }
        }

        double expected = 1.0 / (double) NUM_ROUTES;
        for (MockLatencyWeightedRoute route : routes) {
            assertTrue(Math.abs((double) route.count() / (double) REPEAT - expected) < 0.01);
        }
    }

    @Test
    public void testSkewedRouting() {
        Map<MockLatencyWeightedRoute, Long> latencyMap = new IdentityHashMap<>();
        long latency = 100L;
        double sum = 0.0;
        for (MockLatencyWeightedRoute route : routes) {
            latencyMap.put(route, latency);
            sum += 1.0 / (double) latency;
            latency *= 2;
        }

        Map<MockLatencyWeightedRoute, Double> ratioMap = new IdentityHashMap<>();
        for (Map.Entry<MockLatencyWeightedRoute, Long> entry : latencyMap.entrySet()) {
            ratioMap.put(entry.getKey(), (1.0 / (double) entry.getValue()) / sum);
        }

        for (int i = 0; i < REPEAT; i++) {
            MockLatencyWeightedRoute route = router.getRoute();

            if (route != null) {
                route.incr();
                route.updateExpectedLatency(latencyMap.get(route));

            } else {
                fail();
            }
        }

        for (MockLatencyWeightedRoute route : routes) {
            assertTrue(Math.abs((double) route.count() / (double) REPEAT - ratioMap.get(route)) < 0.01);
        }
    }

    private static class MockLatencyWeightedRoute extends LatencyWeightedRoute {

        private AtomicInteger counter = new AtomicInteger(0);

        @Override
        public boolean isClosed() {
            return false;
        }

        public void incr() {
            counter.incrementAndGet();
        }

        public int count() {
            return counter.get();
        }

    }

}
