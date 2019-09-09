package com.wepay.riff.metrics.jetty9;

import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.core.RatioGauge;
import org.eclipse.jetty.util.annotation.Name;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.util.concurrent.BlockingQueue;

import static com.wepay.riff.metrics.core.MetricRegistry.name;

public class InstrumentedQueuedThreadPool extends QueuedThreadPool {
    private static final int MAX_THREADS = 200;
    private static final int IDLE_TIMEOUT = 60000;

    private final MetricRegistry metricRegistry;
    private String prefix;

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry) {
        this(registry, MAX_THREADS);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
                                        @Name("maxThreads") int maxThreads) {
        this(registry, maxThreads, 8);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
                                        @Name("maxThreads") int maxThreads,
                                        @Name("minThreads") int minThreads) {
        this(registry, maxThreads, minThreads, IDLE_TIMEOUT);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
                                        @Name("maxThreads") int maxThreads,
                                        @Name("minThreads") int minThreads,
                                        @Name("idleTimeout") int idleTimeout) {
        this(registry, maxThreads, minThreads, idleTimeout, null);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
                                        @Name("maxThreads") int maxThreads,
                                        @Name("minThreads") int minThreads,
                                        @Name("idleTimeout") int idleTimeout,
                                        @Name("queue") BlockingQueue<Runnable> queue) {
        this(registry, maxThreads, minThreads, idleTimeout, queue, null);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
                                        @Name("maxThreads") int maxThreads,
                                        @Name("minThreads") int minThreads,
                                        @Name("idleTimeout") int idleTimeout,
                                        @Name("queue") BlockingQueue<Runnable> queue,
                                        @Name("prefix") String prefix) {
        super(maxThreads, minThreads, idleTimeout, queue);
        this.metricRegistry = registry;
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        final String prefix = this.prefix == null ? name(QueuedThreadPool.class, getName()) : name(this.prefix, getName());

        metricRegistry.register("jetty", name(prefix, "utilization"), new RatioGauge() {
            @Override
            protected Ratio getRatio() {
                return Ratio.of(getThreads() - getIdleThreads(), getThreads());
            }
        });
        metricRegistry.register("jetty", name(prefix, "utilization-max"), new RatioGauge() {
            @Override
            protected Ratio getRatio() {
                return Ratio.of(getThreads() - getIdleThreads(), getMaxThreads());
            }
        });
        metricRegistry.register("jetty", name(prefix, "size"), (Gauge<Integer>) this::getThreads);
        metricRegistry.register("jetty", name(prefix, "jobs"), (Gauge<Integer>) () -> {
            // This assumes the QueuedThreadPool is using a BlockingArrayQueue or
            // ArrayBlockingQueue for its queue, and is therefore a constant-time operation.
            return getQueue().size();
        });
    }
}
