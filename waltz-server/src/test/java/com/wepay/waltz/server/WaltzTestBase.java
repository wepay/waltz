package com.wepay.waltz.server;

import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.store.Store;
import com.wepay.waltz.test.mock.MockClusterManager;
import com.wepay.waltz.test.mock.MockStore;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import io.netty.handler.ssl.SslContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class WaltzTestBase {

    protected static final long TIMEOUT = 100000L;
    protected static final int NUM_PARTITIONS = 4;

    protected final PartitionInfo partition0 = new PartitionInfo(0, 99);
    protected final PartitionInfo partition1 = new PartitionInfo(1, 99);
    protected final PartitionInfo partition2 = new PartitionInfo(2, 99);
    protected final PartitionInfo partition3 = new PartitionInfo(3, 99);

    protected final WaltzServerConfig config = new WaltzServerConfig(new Properties());
    protected final PortFinder portFinder = new PortFinder();

    protected void setPartitions(ManagedServer server, PartitionInfo... infos) {
        if (infos != null && infos.length > 0) {
            server.setPartitions(Utils.list(infos));
        } else {
            server.setPartitions(Arrays.asList(partition0, partition1, partition2, partition3));
        }
    }

    protected void setEndpoints(ManagedClient client, ManagedServer server, MockClusterManager clusterManager, PartitionInfo... infos) {
        Map<Endpoint, List<PartitionInfo>> endpoints = new HashMap<>();
        if (infos != null && infos.length > 0) {
            client.setNumPartitions(infos.length);
            endpoints.put(
                clusterManager.endPoint(server),
                Utils.list(infos)
            );
        } else {
            client.setNumPartitions(NUM_PARTITIONS);
            endpoints.put(
                clusterManager.endPoint(server),
                Arrays.asList(partition0, partition1, partition2, partition3)
            );
        }
        client.setEndpoints(endpoints);
    }

    protected WaltzServerRunner getWaltzServerRunner(final SslContext serverSslCtx, final ClusterManager clusterManager) {
        return getWaltzServerRunner(portFinder.getPort(), serverSslCtx, clusterManager, new MockStore());
    }

    protected WaltzServerRunner getWaltzServerRunner(final SslContext serverSslCtx, final ClusterManager clusterManager, final Store store) {
        return getWaltzServerRunner(portFinder.getPort(), serverSslCtx, clusterManager, store);
    }

    protected WaltzServerRunner getWaltzServerRunner(int port, final SslContext serverSslCtx, final ClusterManager clusterManager, final Store store) {
        return new WaltzServerRunner(port, serverSslCtx, config, true) {
            @Override
            protected ZooKeeperClient getZkClient() {
                return null;
            }
            @Override
            protected Store getStore() {
                return store;
            }
            @Override
            protected ClusterManager getClusterManager() {
                return clusterManager;
            }
        };
    }

}
