package com.wepay.waltz.test.util;

import com.wepay.riff.network.ServerSSL;
import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.storage.WaltzStorage;
import com.wepay.waltz.storage.WaltzStorageConfig;
import io.netty.handler.ssl.SslContext;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

public class WaltzStorageRunner extends Runner<WaltzStorage> {

    private final int port;
    private final int adminPort;
    private final Path directory;
    private final WaltzStorageConfig config;
    private final SslContext sslCtx;

    public WaltzStorageRunner(PortFinder portFinder) throws Exception {
        this(portFinder, null, null);
    }

    public WaltzStorageRunner(PortFinder portFinder, WaltzStorageConfig config, Long segmentSizeThreshold) throws Exception {
        this(portFinder.getPort(), portFinder.getPort(), config, segmentSizeThreshold);
    }

    public WaltzStorageRunner(int port, int adminPort) throws Exception {
        this(port, adminPort, null, null);
    }

    public WaltzStorageRunner(int port, int adminPort, WaltzStorageConfig config) throws Exception {
        this(port, adminPort, config, null);
    }

    private WaltzStorageRunner(int port, int adminPort, WaltzStorageConfig config, Long segmentSizeThreshold) throws Exception {
        this.port = port;
        this.adminPort = adminPort;

        if (config == null) {
            this.directory = Files.createTempDirectory("waltz-storage-");

            Properties props = new Properties();

            props.setProperty(WaltzStorageConfig.STORAGE_DIRECTORY, directory.toString());

            if (segmentSizeThreshold != null) {
                props.setProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD, segmentSizeThreshold.toString());
            }

            this.config = new WaltzStorageConfig(props);
            this.sslCtx = ServerSSL.createInsecureContext();

        } else {
            this.directory = FileSystems.getDefault().getPath((String) config.get(WaltzStorageConfig.STORAGE_DIRECTORY));
            this.config = config;
            this.sslCtx = null;
        }
    }

    public Path directory() {
        return directory;
    }

    public int[] checksums(UUID key, int numPartitions) throws Exception {
        return WaltzStorage.checksums(key, numPartitions, config);
    }

    protected WaltzStorage createServer() throws Exception {
        SslContext sslCtx = this.sslCtx != null ? this.sslCtx : ServerSSL.createContext(config.getSSLConfig());
        WaltzStorage server = new WaltzStorage(port, adminPort, sslCtx, config);

        server.setJettyServer();

        return server;
    }

    protected void closeServer() {
        try {
            server.close();
            server = null;

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

}
