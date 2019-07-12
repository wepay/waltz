package com.wepay.waltz.client.internal;

import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.PartitionInfo;

import java.util.List;
import java.util.Map;

public class WaltzManagedClient implements ManagedClient {

    private final InternalRpcClient rpcClient;
    private final InternalStreamClient streamClient;

    public WaltzManagedClient(InternalRpcClient rpcClient, InternalStreamClient streamClient) {
        this.rpcClient = rpcClient;
        this.streamClient = streamClient;
    }

    @Override
    public void setClusterName(String clusterName) {
        rpcClient.setClusterName(clusterName);
        streamClient.setClusterName(clusterName);
    }

    @Override
    public void setClientId(int clientId) {
        rpcClient.setClientId(clientId);
        streamClient.setClientId(clientId);
    }

    @Override
    public void setNumPartitions(int numPartitions) {
        rpcClient.setNumPartitions(numPartitions);
        streamClient.setNumPartitions(numPartitions);
    }

    @Override
    public void setEndpoints(Map<Endpoint, List<PartitionInfo>> endpoints) {
        rpcClient.setEndpoints(endpoints);
        streamClient.setEndpoints(endpoints);
    }

    @Override
    public void removeServer(Endpoint endpoint) {
        rpcClient.removeServer(endpoint);
        streamClient.removeServer(endpoint);
    }

}
