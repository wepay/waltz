package com.wepay.waltz.store.internal;

import com.wepay.riff.network.ClientSSL;
import com.wepay.waltz.server.WaltzServerConfig;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.UUID;

/**
 *  Implements the config needed for the replica connection.
 */
public class ConnectionConfig {

    public final SslContext sslCtx;
    public final UUID key;
    public final int numPartitions;
    public final long initialRetryInterval;
    public final long maxRetryInterval;

    /**
     * Class constructor.
     * @param key The cluster key.
     * @param numPartitions The total number of partitions in the cluster.
     * @param waltzServerConfig The configuration of the {@link com.wepay.waltz.server.WaltzServer}.
     * @throws GeneralSecurityException thrown if failed to create {@link javax.net.ssl.SSLContext}.
     * @throws IOException thrown if any issue occurs.
     */
    public ConnectionConfig(UUID key, int numPartitions, WaltzServerConfig waltzServerConfig) throws GeneralSecurityException, IOException {
        this(
            ClientSSL.createContext(waltzServerConfig.getSSLConfig()),
            key,
            numPartitions,
            (long) waltzServerConfig.get(WaltzServerConfig.INITIAL_RETRY_INTERVAL),
            (long) waltzServerConfig.get(WaltzServerConfig.MAX_RETRY_INTERVAL)
        );
    }

    /**
     * Class constructor.
     * @param sslCtx SSLContext for communication.
     * @param key The cluster key.
     * @param numPartitions The total number of partitions in the cluster.
     * @param initialRetryInterval The initial retry interval.
     * @param maxRetryInterval The maximum retry interval.
     * @throws GeneralSecurityException thrown if failed to create {@link javax.net.ssl.SSLContext}.
     * @throws IOException thrown if any issue occurs.
     */
    public ConnectionConfig(
        SslContext sslCtx,
        UUID key,
        int numPartitions,
        final long initialRetryInterval,
        final long maxRetryInterval
    ) throws GeneralSecurityException, IOException {
        this.sslCtx = sslCtx != null ? sslCtx : ClientSSL.createInsecureContext();
        this.key = key;
        this.numPartitions = numPartitions;
        this.initialRetryInterval = initialRetryInterval;
        this.maxRetryInterval = maxRetryInterval;
    }
}
