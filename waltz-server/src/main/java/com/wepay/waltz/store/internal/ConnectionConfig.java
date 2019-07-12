package com.wepay.waltz.store.internal;

import com.wepay.riff.network.ClientSSL;
import com.wepay.waltz.server.WaltzServerConfig;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.UUID;

public class ConnectionConfig {

    public final SslContext sslCtx;
    public final UUID key;
    public final int numPartitions;
    public final long initialRetryInterval;
    public final long maxRetryInterval;

    public ConnectionConfig(UUID key, int numPartitions, WaltzServerConfig waltzServerConfig) throws GeneralSecurityException, IOException {
        this(
            ClientSSL.createContext(waltzServerConfig.getSSLConfig()),
            key,
            numPartitions,
            (long) waltzServerConfig.get(WaltzServerConfig.INITIAL_RETRY_INTERVAL),
            (long) waltzServerConfig.get(WaltzServerConfig.MAX_RETRY_INTERVAL)
        );
    }

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
