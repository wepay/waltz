package com.wepay.waltz.test.util;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.server.WaltzServer;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.server.internal.Partition;
import com.wepay.waltz.store.Store;
import com.wepay.waltz.store.exception.StoreException;
import com.wepay.waltz.store.internal.StoreImpl;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.clustermgr.internal.ClusterManagerImpl;
import com.wepay.zktools.clustermgr.internal.DynamicPartitionAssignmentPolicy;
import com.wepay.zktools.clustermgr.internal.PartitionAssignmentPolicy;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;

public class WaltzServerRunner extends Runner<WaltzServer> {

    private static final Logger logger = Logging.getLogger(WaltzServerRunner.class);

    private final PartitionAssignmentPolicy partitionAssignmentPolicy = new DynamicPartitionAssignmentPolicy();
    private final WaltzServerConfig config;

    private final int port;
    private final SslContext sslCtx;
    private final boolean messageBuffering;

    private ZooKeeperClient zkClient = null;
    private ClusterManager clusterManager = null;

    public WaltzServerRunner(int port, SslContext sslCtx, WaltzServerConfig config, boolean messageBuffering) {
        this.port = port;
        this.sslCtx = sslCtx;
        this.config = config;
        this.messageBuffering = messageBuffering;
    }

    protected WaltzServer createServer() throws Exception {
        zkClient = getZkClient();
        Store store = getStore();
        clusterManager = getClusterManager();

        if (messageBuffering) {
            return new WaltzServer(port, sslCtx, store, clusterManager, config) {
                protected Partition createPartition(PartitionInfo info) {
                    return new PartitionWithMessageBuffering(
                        info.partitionId,
                        store.getPartition(info.partitionId, info.generation),
                        feedCache.getPartition(info.partitionId),
                        transactionFetcher,
                        config);
                }
            };
        } else {
            WaltzServer server = new WaltzServer(port, sslCtx, store, clusterManager, config);
            server.setJettyServer(zkClient);
            return server;
        }
    }

    protected void closeServer() {
        try {
            if (server != null) {
                server.close();
                server = null;
            }
        } catch (Throwable ex) {
            logger.error("failed to close WaltzServer", ex);

        } finally {
            try {
                if (clusterManager != null) {
                    clusterManager.close();
                    clusterManager = null;
                }
            } catch (Throwable ex) {
                logger.error("failed to close ClusterManager", ex);
            }

            try {
                if (zkClient != null) {
                    zkClient.close();
                    zkClient = null;
                }
            } catch (Throwable ex) {
                logger.error("failed to close ZooKeeperClient", ex);
            }
        }
    }

    protected ZooKeeperClient getZkClient() throws ZooKeeperClientException {
        return new ZooKeeperClientImpl(
            (String) config.get(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING),
            (int) config.get(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT)
        );
    }

    protected ClusterManager getClusterManager() throws ClusterManagerException {
        ZNode root = new ZNode((String) config.get(WaltzServerConfig.CLUSTER_ROOT));

        return new ClusterManagerImpl(zkClient, root, partitionAssignmentPolicy);
    }

    protected Store getStore() throws StoreException {
        ZNode root = new ZNode((String) config.get(WaltzServerConfig.CLUSTER_ROOT));
        ZNode storeRoot = new ZNode(root, "store");

        return new StoreImpl(zkClient, storeRoot, config);
    }

    public void closeNetworkServer() {
        try {
            server.getNetworkServer().close();
        } catch (Exception ex) {
            logger.error("failed to close the network server", ex);
        }
    }
}
