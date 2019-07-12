package com.wepay.waltz.server.health;

import com.wepay.waltz.server.WaltzServer;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperSession;

import java.util.Map;

public class HealthCheck {

    private final WaltzServer waltzServer;

    private final int zkWait;

    private long zkDisconnectedSince = System.currentTimeMillis();

    public HealthCheck(ZooKeeperClient zkClient, WaltzServer waltzServer) {
        this.waltzServer = waltzServer;
        this.zkWait = zkClient.getSessionTimeout() * 2;

        zkClient.onDisconnected(this::onDisconnected);
        zkClient.onConnected(this::onConnected);
    }

    public boolean zkIsHealthy() {
        return zkDisconnectedSince < 0 || System.currentTimeMillis() < zkDisconnectedSince + zkWait;
    }

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
