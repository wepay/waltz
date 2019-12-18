package com.wepay.waltz.client.internal;

import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.store.Store;
import com.wepay.waltz.test.mock.MockClusterManager;
import com.wepay.waltz.test.mock.MockStore;
import com.wepay.waltz.test.mock.MockWaltzClientCallbacks;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import io.netty.handler.ssl.SslContext;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class InternalClientTestBase {

    private final PortFinder portFinder = new PortFinder();
    private final WaltzServerConfig config = new WaltzServerConfig(Collections.emptyMap());
    private final PartitionInfo partitionInfo0 = new PartitionInfo(0, 99);
    private final PartitionInfo partitionInfo1 = new PartitionInfo(1, 99);
    private final PartitionInfo partitionInfo2 = new PartitionInfo(2, 99);
    private final AtomicInteger clientId = new AtomicInteger(0);

    protected final Set<Integer> allPartitions = Utils.set(0, 1, 2);
    protected final List<InternalBaseClient> clients = new LinkedList<>();

    protected SslContext clientSslCtx;
    protected MockClusterManager clusterManager;
    protected MockStore store;
    protected List<PartitionInfo> partitionInfoList;
    protected WaltzServerRunner serverRunner;
    protected Map<Endpoint, List<PartitionInfo>> endpoints;
    protected ManagedServer managedServer;

    @Before
    public void setup() throws Exception {
        clientSslCtx = ClientSSL.createInsecureContext();

        clusterManager = new MockClusterManager(1);

        store = new MockStore();
        partitionInfoList = Arrays.asList(partitionInfo0, partitionInfo1, partitionInfo2);

        serverRunner = new WaltzServerRunner(portFinder.getPort(), null, config, true) {
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
        serverRunner.startAsync();
        serverRunner.awaitStart();

        managedServer = clusterManager.managedServers().iterator().next();
        managedServer.setPartitions(partitionInfoList);

        endpoints = new HashMap<>();
        endpoints.put(clusterManager.endPoint(managedServer), partitionInfoList);
    }

    protected MockWaltzClientCallbacks getCallbacks() {
        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks();
        callbacks.setClientHighWaterMark(0, -1L).setClientHighWaterMark(1, -1L).setClientHighWaterMark(2, -1L);
        return callbacks;
    }

    protected InternalRpcClient getInternalRpcClient(int maxConcurrentTransactions) {
        InternalRpcClient internalRpcClient = new InternalRpcClient(clientSslCtx, maxConcurrentTransactions, getCallbacks());
        clients.add(internalRpcClient);

        internalRpcClient.setNumPartitions(partitionInfoList.size());
        internalRpcClient.setClientId(clientId.incrementAndGet());
        internalRpcClient.setEndpoints(endpoints);

        return internalRpcClient;
    }

    protected InternalStreamClient getInternalStreamClient(boolean autoMount, int maxConcurrentTransactions, InternalRpcClient rpcClient) {
        InternalStreamClient internalStreamClient = new InternalStreamClient(autoMount, clientSslCtx, maxConcurrentTransactions, getCallbacks(), rpcClient, null);
        clients.add(internalStreamClient);

        internalStreamClient.setClientId(clientId.incrementAndGet());
        internalStreamClient.setNumPartitions(partitionInfoList.size());
        internalStreamClient.setEndpoints(endpoints);

        return internalStreamClient;
    }

    @After
    public void teardown() throws Exception {
        for (InternalBaseClient client : clients) {
            client.close();
        }
        clusterManager.close();
        serverRunner.stop();
        store.close();
    }

}
