package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ManagedClient;

/**
 * The interface for WaltzClient drivers.
 */
public interface WaltzClientDriver {

    /**
     * Initializes the driver with the given callbacks and the config.
     *
     * @param callbacks the Waltz client callbacks.
     * @param config the Waltz client config.
     */
    void initialize(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception;

    /**
     * @return the ClusterManager.
     */
    ClusterManager getClusterManager();

    /**
     * @return the ManagedClient.
     */
    ManagedClient getManagedClient();

    /**
     * @return the RpcClient.
     */
    RpcClient getRpcClient();

    /**
     * @return the StreamClient.
     */
    StreamClient getStreamClient();

    /**
     * Closes the driver.
     */
    void close();
}
