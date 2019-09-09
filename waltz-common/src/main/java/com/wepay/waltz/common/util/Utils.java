package com.wepay.waltz.common.util;

import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.graphite.Graphite;
import com.wepay.riff.metrics.graphite.GraphiteReporter;
import com.wepay.riff.metrics.graphite.GraphiteReporterConfig;
import com.wepay.riff.metrics.graphite.GraphiteSender;
import com.wepay.riff.metrics.graphite.GraphiteUDP;
import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.network.SSLConfig;
import com.wepay.riff.util.Logging;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public final class Utils {

    private static final Logger logger = Logging.getLogger(Utils.class);

    @SuppressWarnings("unchecked")
    public static <T> List<T> list(T... elem) {
        return Arrays.asList(elem);
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> set(T... elem) {
        return new HashSet<>(Arrays.asList(elem));
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> map(K key, V value) {
        return entriesToMap(new AbstractMap.SimpleEntry<>(key, value));
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2) {
        return entriesToMap(
                new AbstractMap.SimpleEntry<>(key1, value1),
                new AbstractMap.SimpleEntry<>(key2, value2)
        );
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3) {
        return entriesToMap(
                new AbstractMap.SimpleEntry<>(key1, value1),
                new AbstractMap.SimpleEntry<>(key2, value2),
                new AbstractMap.SimpleEntry<>(key3, value3)
        );
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        return entriesToMap(
                new AbstractMap.SimpleEntry<>(key1, value1),
                new AbstractMap.SimpleEntry<>(key2, value2),
                new AbstractMap.SimpleEntry<>(key3, value3),
                new AbstractMap.SimpleEntry<>(key4, value4)
        );
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5) {
        return entriesToMap(
                new AbstractMap.SimpleEntry<>(key1, value1),
                new AbstractMap.SimpleEntry<>(key2, value2),
                new AbstractMap.SimpleEntry<>(key3, value3),
                new AbstractMap.SimpleEntry<>(key4, value4),
                new AbstractMap.SimpleEntry<>(key5, value5)
        );
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> entriesToMap(Map.Entry<K, V>... entries) {
        return Stream.of(entries).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    private Utils() {
    }

    public static void removeDirectory(File file) {
        if (file != null) {
            if (file.isDirectory()) {
                File[] list = file.listFiles();
                if (list != null) {
                    for (File f : list) {
                        Utils.removeDirectory(f);
                    }
                }
            }
            if (!file.delete()) {
                logger.warn("file not deleted: " + file.toPath());
            }
        }
    }

    public static int checksum(byte[] data) {
        return checksum(data, 0, data.length);
    }

    public static int checksum(byte[] data, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(data, offset, length);
        return (int) crc32.getValue();
    }

    public static void verifyChecksum(int msgType, byte[] data, int checksum) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        if (checksum != (int) crc32.getValue()) {
            throw new IllegalStateException("checksum error: " + msgType);
        }
    }

    public static int checksum(ByteBuffer byteBuffer, int position, int length) {
        byte[] bytes = new byte[length];

        ByteBuffer temp = byteBuffer.duplicate();
        temp.position(position);
        temp.get(bytes);

        return checksum(bytes);
    }

    /**
     * Return an {@link SslContext} containing all SSL configurations parsed
     * from the YAML file path
     * <p>
     * See {@link SSLConfig} class for a list of valid config names
     *
     * @param sslConfigPath the SSL config file path required for the storage node
     * @param sslConfigPrefix the prefix to use to parse SSL config file
     * @return SslContext
     */
    public static SslContext getSslContext(String sslConfigPath, String sslConfigPrefix) throws GeneralSecurityException, IOException {
        if (sslConfigPath != null && sslConfigPrefix != null) {
            Yaml yaml = new Yaml();
            try (FileInputStream fileInputStream = new FileInputStream(sslConfigPath)) {
                SSLConfig sslConfig = new SSLConfig(sslConfigPrefix, yaml.load(fileInputStream));
                return ClientSSL.createContext(sslConfig);
            }
        } else {
            return ClientSSL.createInsecureContext();
        }
    }

    /**
     * Returns a GraphiteReporter that has not yet been started. If no GraphiteReporter configuration exists (i.e. the
     * hostname is not set), then empty is returned.
     *
     * @param metricRegistry The metrics registry to attach the reporter to.
     * @param graphiteReporterConfig The GraphiteReporterConfig to use when configuring the builder and reporter.
     * @return Empty if Graphite's hostname is not set, else a GraphiteReporter that hasn't been started yet.
     */
    public static Optional<GraphiteReporter> getGraphiteReporter(MetricRegistry metricRegistry, GraphiteReporterConfig graphiteReporterConfig) {
        boolean isGraphiteEnabled = graphiteReporterConfig.getOpt(GraphiteReporterConfig.HOSTNAME).isPresent();

        if (isGraphiteEnabled) {
            String hostname = (String) graphiteReporterConfig.get(GraphiteReporterConfig.HOSTNAME);
            int port = (int) graphiteReporterConfig.get(GraphiteReporterConfig.PORT);
            String prefix = (String) graphiteReporterConfig.get(GraphiteReporterConfig.PREFIX);
            boolean useUdp = (boolean) graphiteReporterConfig.get(GraphiteReporterConfig.USE_UDP);

            logger.info("Graphite reporter enabled with: host={}, port={}, prefix={}, udp={}", hostname, port, prefix, useUdp);

            GraphiteSender sender = useUdp ? new GraphiteUDP(hostname, port) : new Graphite(hostname, port);
            GraphiteReporter reporter = GraphiteReporter
                    .forRegistry(metricRegistry)
                    .prefixedWith(prefix)
                    .build(sender);

            return Optional.of(reporter);
        }

        return Optional.empty();
    }

    public static Map<String, String> getBuildInfoMap(Class<?> clazz) {
        Map<String, String> buildInfo = new HashMap<>();

        buildInfo.put("version", clazz.getPackage().getImplementationVersion());
        buildInfo.put("service_name", clazz.getPackage().getImplementationTitle());

        return buildInfo;
    }
}
