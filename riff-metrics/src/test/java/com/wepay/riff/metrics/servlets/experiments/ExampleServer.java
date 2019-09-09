package com.wepay.riff.metrics.servlets.experiments;

import com.wepay.riff.metrics.core.Counter;
import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.health.HealthCheckRegistry;
import com.wepay.riff.metrics.jetty9.InstrumentedConnectionFactory;
import com.wepay.riff.metrics.jetty9.InstrumentedHandler;
import com.wepay.riff.metrics.jetty9.InstrumentedQueuedThreadPool;
import com.wepay.riff.metrics.servlets.AdminServlet;
import com.wepay.riff.metrics.servlets.HealthCheckServlet;
import com.wepay.riff.metrics.servlets.MetricsServlet;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.ThreadPool;

import static com.wepay.riff.metrics.core.MetricRegistry.name;

public final class ExampleServer {
    private static final MetricRegistry REGISTRY = MetricRegistry.getInstance();
    private static final Counter COUNTER_1 = REGISTRY.counter("group", name(ExampleServer.class, "wah", "doody"));
    private static final Counter COUNTER_2 = REGISTRY.counter("group", name(ExampleServer.class, "woo"));

    private ExampleServer() {
        //not called
    }

    static {
        REGISTRY.register("group", name(ExampleServer.class, "boo"), (Gauge<Integer>) () -> {
            throw new RuntimeException("asplode!");
        });
    }

    public static void main(String[] args) throws Exception {
        COUNTER_1.inc();
        COUNTER_2.inc();

        final ThreadPool threadPool = new InstrumentedQueuedThreadPool(REGISTRY);
        final Server server = new Server(threadPool);

        final Connector connector = new ServerConnector(server, new InstrumentedConnectionFactory(
                new HttpConnectionFactory(), REGISTRY.timer("group", "http.connection")));
        server.addConnector(connector);

        final ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/initial");
        context.setAttribute(MetricsServlet.METRICS_REGISTRY, REGISTRY);
        context.setAttribute(HealthCheckServlet.HEALTH_CHECK_REGISTRY, new HealthCheckRegistry());

        final ServletHolder holder = new ServletHolder(new AdminServlet());
        context.addServlet(holder, "/dingo/*");

        final InstrumentedHandler handler = new InstrumentedHandler(REGISTRY);
        handler.setHandler(context);
        server.setHandler(handler);

        server.start();
        server.join();
    }
}
