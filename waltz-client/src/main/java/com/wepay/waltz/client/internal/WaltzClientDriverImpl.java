package com.wepay.waltz.client.internal;

import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.network.MessageProcessingThreadPool;
import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.DynamicPartitionAssignmentPolicy;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;

public class WaltzClientDriverImpl implements WaltzClientDriver {

    private static final Logger logger = Logging.getLogger(WaltzClientDriverImpl.class);

    private InternalRpcClient rpcClient;
    private InternalStreamClient streamClient;
    private ZooKeeperClientImpl zooKeeperClient;
    private ClusterManager clusterManager;
    private ManagedClient managedClient;
    private MessageProcessingThreadPool messageProcessingThreadPool;

    @Override
    public void initialize(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception {
        boolean autoMount = (boolean) config.get(WaltzClientConfig.AUTO_MOUNT);

        SslContext sslCtx = ClientSSL.createContext(config.getSSLConfig());
        if (sslCtx == null) {
            sslCtx = ClientSSL.createInsecureContext();
        }

        this.messageProcessingThreadPool = new MessageProcessingThreadPool(
            (int) config.get(WaltzClientConfig.NUM_CONSUMER_THREADS)
        ).open();

        this.rpcClient = new InternalRpcClient(sslCtx, callbacks);
        this.streamClient = new InternalStreamClient(autoMount, sslCtx, callbacks, this.rpcClient, this.messageProcessingThreadPool);

        this.zooKeeperClient = new ZooKeeperClientImpl(
            (String) config.get(WaltzClientConfig.ZOOKEEPER_CONNECT_STRING),
            (int) config.get(WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT)
        );

        this.clusterManager = new ClusterManagerImpl(
            zooKeeperClient,
            new ZNode((String) config.get(WaltzClientConfig.CLUSTER_ROOT)),
            new DynamicPartitionAssignmentPolicy()
        );

        this.managedClient = new WaltzManagedClient(this.rpcClient, this.streamClient);
        this.clusterManager.manage(this.managedClient);
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
            if (clusterManager != null) {
                clusterManager.close();
            }
        } catch (Throwable ex) {
            logger.error("failed to close clusterManager", ex);
        }
        try {
            if (zooKeeperClient != null) {
                zooKeeperClient.close();
            }
        } catch (Throwable ex) {
            logger.error("failed to close zooKeeperClient", ex);
        }
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

}
