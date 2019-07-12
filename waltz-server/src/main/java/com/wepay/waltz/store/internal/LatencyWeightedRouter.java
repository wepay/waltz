package com.wepay.waltz.store.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * This class provides a latency based routing mechanism. Each route is weighted by its cumulative latency.
 * The route with the smallest weight is chosen by {@link LatencyWeightedRouter#getRoute()}. The weight of chosen route
 * is immediately updated by calling {@link LatencyWeightedRoute#updateWeight()}.
 * @param <R> The implementation class of LatencyWeightedRoute
 */
public class LatencyWeightedRouter<R extends LatencyWeightedRoute> {

    public static final long MAX_LATENCY = 3000;
    public static final long DEFAULT_LATENCY = 10;
    private static final Comparator<LatencyWeightedRoute> COMPARATOR = Comparator.comparingLong(LatencyWeightedRoute::weight);

    private final PriorityQueue<R> pq;

    public LatencyWeightedRouter(Collection<R> routes) {
        this.pq = new PriorityQueue<>(routes.size(), COMPARATOR);
        this.pq.addAll(routes);
    }

    /**
     * Returns a route.
     * @return a route
     */
    public R getRoute() {
        synchronized (pq) {
            while (!pq.isEmpty()) {
                // Get the route with the smallest weight.
                R route = pq.poll();

                if (!route.isClosed()) {
                    // Update the weight and put the route back in the priority queue immediately.
                    // This allows concurrent uses of the same route.
                    route.updateWeight();
                    pq.offer(route);

                    return route;
                }
            }
            return null;
        }
    }

}
