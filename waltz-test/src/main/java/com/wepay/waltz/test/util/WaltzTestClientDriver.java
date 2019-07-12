package com.wepay.waltz.test.util;

import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.InternalRpcClient;
import com.wepay.waltz.client.internal.InternalStreamClient;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.client.internal.StreamClient;
import com.wepay.waltz.client.internal.WaltzClientDriver;
import com.wepay.waltz.client.internal.WaltzManagedClient;
import com.wepay.waltz.test.mock.MockClusterManager;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ManagedClient;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;

public class WaltzTestClientDriver implements WaltzClientDriver {

    private static final Logger logger = Logging.getLogger(WaltzTestClientDriver.class);

    private final InternalRpcClient rpcClient;
    private final InternalStreamClient streamClient;
    private final ClusterManager clusterManager;
    private final ManagedClient managedClient;
    private final MessageProcessingThreadPool messageProcessingThreadPool;

    public WaltzTestClientDriver(
        boolean autoMount,
        SslContext sslCtx,
        WaltzClientCallbacks callbacks,
        MockClusterManager clusterManager
    ) throws Exception {
        if (sslCtx == null) {
            sslCtx = ClientSSL.createInsecureContext();
        }

        this.messageProcessingThreadPool = new MessageProcessingThreadPool(10).open();
        this.rpcClient = new InternalRpcClient(sslCtx, callbacks);
        this.streamClient = new InternalStreamClient(autoMount, sslCtx, callbacks, this.rpcClient, this.messageProcessingThreadPool);
        this.managedClient = createManagedClient(this.rpcClient, this.streamClient);
        this.clusterManager = clusterManager;
        this.clusterManager.manage(this.managedClient);
    }

    @Override
    public void initialize(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    @Override
    public ManagedClient getManagedClient() {
        return managedClient;
    }

    @Override
    public RpcClient getRpcClient() {
        return rpcClient;
    }

    @Override
    public StreamClient getStreamClient() {
        return streamClient;
    }

    @Override
    public void close() {
        try {
            if (streamClient != null) {
                streamClient.close();
            }
        } catch (Throwable ex) {
            logger.error("failed to close stream client", ex);
        }
        try {
            if (rpcClient != null) {
                rpcClient.close();
            }
        } catch (Throwable ex) {
            logger.error("failed to close rpc client", ex);
        }
        try {
            if (messageProcessingThreadPool != null) {
                messageProcessingThreadPool.close();
            }
        } catch (Throwable ex) {
            logger.error("failed to close messageProcessingThreadPool", ex);
        }
    }

    protected ManagedClient createManagedClient(InternalRpcClient rpcClient, InternalStreamClient streamClient) {
        return new WaltzManagedClient(rpcClient, streamClient);
    }

}
