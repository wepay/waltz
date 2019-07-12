package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ManagedClient;

public interface WaltzClientDriver {

    /**
     * Initializes the driver with the given callbacks and the config
     * @param callbacks the Waltz client callbacks
     * @param config the Waltz client config
     */
    void initialize(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception;

    /**
     * Returns the cluster manager
     * @return ClusterManager
     */
    ClusterManager getClusterManager();

    /**
     * Returns the managed client
     * @return ManagedClient
     */
    ManagedClient getManagedClient();

    /**
     * Returns the RPC client
     * @return RpcCLient
     */
    RpcClient getRpcClient();

    /**
     * Returns the Stream client
     * @return StreamClient
     */
    StreamClient getStreamClient();

    /**
     * Closes the driver
     */
    void close();
}
