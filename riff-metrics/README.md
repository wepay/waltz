## Riff Metrics

This is an integrated metrics library built on [Dropwizard/Metrics]. Developers can expose metrics via console, [HTTP], [JMX]
or [StatsD]. And eventually allow monitoring with Grafana/Prometheus.

### User Guide

The 5 types of metrics supported: Meter, Counter, Timer, Histogram, Gauge.<br />
The 3 types of reporter supported: ConsoleReporter, JmxReporter, StatsDReporter.<br />
The 5 types of servlet supported: AdminServlet, PingServlet, MetricsServlet, HealthCheckServlet, BuildInfoServlet.

### Reference 

This libraray is built Dropwizard/Metrics open source project, including [metrics-core],
[metrics-jmx], and third-party [metrics-statsd]. 

  [HTTP]: https://metrics.dropwizard.io/4.0.0/getting-started.html#reporting-via-http
  [JMX]: https://metrics.dropwizard.io/4.0.0/getting-started.html#reporting-via-jmx
  [StatsD]: https://github.com/etsy/statsd/
  [Dropwizard/Metrics]: https://github.com/dropwizard/metrics
  [metrics-core]: https://github.com/dropwizard/metrics/tree/4.1-development/metrics-core
  [metrics-jmx]: https://github.com/dropwizard/metrics/tree/4.1-development/metrics-jmx
  [metrics-statsd]: https://github.com/ReadyTalk/metrics-statsd
