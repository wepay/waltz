package com.wepay.waltz.server;

import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.client.internal.StreamClient;
import com.wepay.waltz.client.internal.WaltzClientDriver;
import com.wepay.waltz.test.util.ProxyServer;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.PartitionInfo;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProxyClientDriver implements WaltzClientDriver {

    private final WaltzClientDriver driver;
    private final ProxyServer proxyServer;
    private final Endpoint proxyEndpoint;
    private final Endpoint realEndpoint;

    public ProxyClientDriver(WaltzClientDriver driver, int proxyPort, int realPort) throws Exception {
        this.driver = driver;

        String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        this.proxyEndpoint = new Endpoint(localhost, proxyPort);
        this.realEndpoint = new Endpoint(localhost, realPort);
        this.proxyServer = new ProxyServer(proxyPort, localhost, realPort);
    }

    public void disconnect() {
        proxyServer.disconnectAll();
    }

    @Override
    public void initialize(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception {
        driver.initialize(callbacks, config);
    }

    @Override
    public ClusterManager getClusterManager() {
        return driver.getClusterManager();
    }

    @Override
    public ManagedClient getManagedClient() {
        return new ProxyManagedClient(driver.getManagedClient());
    }

    @Override
    public RpcClient getRpcClient() {
        return driver.getRpcClient();
    }

    @Override
    public StreamClient getStreamClient() {
        return driver.getStreamClient();
    }

    @Override
    public void close() {
        driver.close();
        proxyServer.close();
    }

    private class ProxyManagedClient implements ManagedClient {

        private final ManagedClient managedClient;

        ProxyManagedClient(ManagedClient managedClient) {
            this.managedClient = managedClient;
        }

        @Override
        public void setClusterName(String s) {
            managedClient.setClusterName(s);
        }

        @Override
        public void setClientId(int i) {
            managedClient.setClientId(i);
        }

        @Override
        public void setNumPartitions(int i) {
            managedClient.setNumPartitions(i);
        }

        @Override
        public void removeServer(Endpoint endpoint) {
            if (endpoint.equals(realEndpoint)) {
                managedClient.removeServer(proxyEndpoint);
            } else {
                managedClient.removeServer(endpoint);
            }
        }

        @Override
        public void setEndpoints(Map<Endpoint, List<PartitionInfo>> map) {
            Map<Endpoint, List<PartitionInfo>> proxyMap = new HashMap<>(map);

            List<PartitionInfo> partitionInfoList = proxyMap.remove(realEndpoint);
            if (partitionInfoList != null) {
                proxyMap.put(proxyEndpoint, partitionInfoList);
            }

            managedClient.setEndpoints(proxyMap);
        }
    }

}
