package com.wepay.waltz.client.internal.mock;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.client.internal.StreamClient;
import com.wepay.waltz.client.internal.WaltzClientDriver;
import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.clustermgr.internal.PartitionAssignment;
import com.wepay.zktools.clustermgr.internal.ServerDescriptor;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockDriver implements WaltzClientDriver {

    private static final Logger logger = Logging.getLogger(MockDriver.class);
    private static final String CLUSTER_NAME = "mock";

    private final int clientId;
    private final Map<Integer, MockServerPartition> serverPartitions;
    private final ClusterManager clusterManager;
    private final ManagedClient managedClient;

    private MockRpcClient rpcClient = null;
    private MockStreamClient streamClient = null;

    public MockDriver() {
        this(0, Collections.singletonMap(0, new MockServerPartition(0)));
    }

    public MockDriver(int clientId, Map<Integer, MockServerPartition> serverPartitions) {
        this.clientId = clientId;
        this.serverPartitions = serverPartitions;
        this.clusterManager = new DummyClusterManager();
        this.managedClient = new DummyManagedClient();
    }

    /**
     * Initializes the driver with the given callbacks and the config
     * @param callbacks the Waltz client callbacks
     * @param config the Waltz client config
     */
    public void initialize(WaltzClientCallbacks callbacks, WaltzClientConfig config) throws Exception {
        boolean autoMount = (boolean) config.get(WaltzClientConfig.AUTO_MOUNT);
        rpcClient = new MockRpcClient(clientId, serverPartitions);
        streamClient = new MockStreamClient(clientId, CLUSTER_NAME, autoMount, serverPartitions, rpcClient, callbacks);
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
        for (MockServerPartition partition : serverPartitions.values()) {
            partition.close();
        }
        try {
            clusterManager.close();
        } catch (Throwable ex) {
            logger.error("failed to close clusterManager", ex);
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
    }

    /**
     * Adds a random delay time in transaction processing on server side.
     * @param maxDelayMillis the max time of delay in milliseconds
     */
    public void setMaxDelay(long maxDelayMillis) {
        serverPartitions.values().forEach(p -> p.setMaxDelay(maxDelayMillis));
    }

    /**
     * Adds random append failures.
     * @param faultRate the fault rate
     */
    public void setFaultRate(double faultRate) {
        serverPartitions.values().forEach(p -> p.setFaultRate(faultRate));
    }

    /**
     * Forces the next append request fail.
     */
    public void forceNextAppendFail() {
        streamClient.forceNextAppendFail();
    }

    /**
     * Suspends the feed.
     */
    public void suspendFeed() {
        streamClient.suspendFeed();
    }

    /**
     * Resumes the feed.
     */
    public void resumeFeed() {
        streamClient.resumeFeed();
    }

    private class DummyClusterManager implements ClusterManager {
        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public String clusterName() {
            return CLUSTER_NAME;
        }

        @Override
        public int numPartitions() {
            return serverPartitions.size();
        }

        @Override
        public Set<ServerDescriptor> serverDescriptors() throws ClusterManagerException {
            throw new UnsupportedOperationException(this.getClass().getSimpleName()
                    + "does not create server descriptors");
        }

        @Override
        public PartitionAssignment partitionAssignment() throws ClusterManagerException {
            throw new UnsupportedOperationException(this.getClass().getSimpleName() + "does not assign partitions");
        }

        @Override
        public void manage(ManagedClient managedClient) throws ClusterManagerException {
            // Do nothing
        }

        @Override
        public void manage(ManagedServer managedServer) throws ClusterManagerException {
            // Do nothing
        }

        @Override
        public void unmanage(ManagedClient managedClient) throws ClusterManagerException {
            // Do nothing
        }

        @Override
        public void unmanage(ManagedServer managedServer) throws ClusterManagerException {
            // Do nothing
        }
    }

    private static class DummyManagedClient implements ManagedClient {
        @Override
        public void setClusterName(String s) {
            // Do nothing
        }

        @Override
        public void setClientId(int i) {
            // Do nothing
        }

        @Override
        public void setNumPartitions(int i) {
            // Do nothing
        }

        @Override
        public void removeServer(Endpoint endpoint) {
            // Do nothing
        }

        @Override
        public void setEndpoints(Map<Endpoint, List<PartitionInfo>> map) {
            // Do nothing
        }
    }

}
