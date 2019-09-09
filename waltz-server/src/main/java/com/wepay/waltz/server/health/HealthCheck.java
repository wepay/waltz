package com.wepay.waltz.server.health;

import com.wepay.waltz.server.WaltzServer;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperSession;

import java.util.Map;

/**
 * This class provides health check for a given {@link WaltzServer}.
 */
public class HealthCheck {

    private final WaltzServer waltzServer;

    private final int zkWait;

    private long zkDisconnectedSince = System.currentTimeMillis();

    /**
     * Class Constructor.
     * @param zkClient The ZooKeeperClient used for the Waltz Cluster.
     * @param waltzServer The Waltz server to which the health check has to be done.
     */
    public HealthCheck(ZooKeeperClient zkClient, WaltzServer waltzServer) {
        this.waltzServer = waltzServer;
        this.zkWait = zkClient.getSessionTimeout() * 2;

        zkClient.onDisconnected(this::onDisconnected);
        zkClient.onConnected(this::onConnected);
    }

    /**
     * Checks if the ZooKeeper connection is still active.
     * @return True if ZooKeeper connection is active, otherwise returns False.
     */
    public boolean zkIsHealthy() {
        return zkDisconnectedSince < 0 || System.currentTimeMillis() < zkDisconnectedSince + zkWait;
    }

    /**
     * Checks if each partition in the Waltz server is healthy.
     * @return Map of partition IDs (of the given Waltz server) and its health status. Health
     * status is True if the partition is healthy, otherwise False.
     */
    public Map<Integer, Boolean> getPartitionHealth() {
        return waltzServer.getPartitionHealth();
    }

    private void onDisconnected() {
        zkDisconnectedSince = System.currentTimeMillis();
    }

    private void onConnected(ZooKeeperSession s) {
        zkDisconnectedSince = -1L;
    }
}
