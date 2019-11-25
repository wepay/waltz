package com.wepay.waltz.demo;

import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.jmx.JmxReporter;
import com.wepay.riff.network.ClientSSL;
import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.storage.WaltzStorageConfig;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.metadata.StoreParams;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import com.wepay.waltz.test.util.ZooKeeperServerRunner;
import com.wepay.waltz.tools.zk.ZooKeeperCli;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.wepay.waltz.demo.DemoConst.WALTZ_REPLICA1_ADMIN_PORT;

public final class DemoServers extends DemoAppBase {

    public static final int ZK_PORT = 7000;
    public static final int ZK_SESSION_TIMEOUT = 30000;
    public static final ZNode CLUSTER_ROOT_ZNODE = new ZNode("/demo");
    public static final String CLUSTER_NAME = "demo cluster";

    private int serverPort;
    private int serverJettyPort;
    private int storageJettyPort;
    private static final int STORAGE_GROUP_ID = 0;
    private static final String workDirName = "demo-test";
    private static final long segmentSizeThreshold = 400L;

    private final Map<String, Integer> storageGroupMap;
    private final Map<String, Integer> storageConnectionMap;

    private ZooKeeperServerRunner zooKeeperServerRunner;
    private WaltzStorageRunner[] waltzStorageRunners;
    private WaltzServerRunner waltzServerRunner;

    private final Path storageDir;

    DemoServers() throws Exception {
        super(null);

        PortFinder portFinder = new PortFinder();
        serverPort = portFinder.getPort();
        serverJettyPort = portFinder.getPort();
        storageJettyPort = portFinder.getPort();

        zooKeeperServerRunner = new ZooKeeperServerRunner(ZK_PORT);

        storageDir = Files.createTempDirectory(workDirName).resolve("storage-" + storageJettyPort);
        if (!Files.exists(storageDir)) {
            Files.createDirectory(storageDir);
        }

        String host = InetAddress.getLocalHost().getCanonicalHostName();
        storageGroupMap = Utils.map(
            host + ":" + DemoConst.WALTZ_REPLICA1_PORT, STORAGE_GROUP_ID
        );
        storageConnectionMap = Utils.map(
            host + ":" + DemoConst.WALTZ_REPLICA1_PORT, WALTZ_REPLICA1_ADMIN_PORT
        );
    }

    void startServers() throws Exception {
        // JmxReporter
        JmxReporter.forRegistry(MetricRegistry.getInstance()).build().start();

        // ZooKeeper
        zooKeeperServerRunner.start();

        // Waltz
        StoreParams storeParams;
        try (ZooKeeperClient zkClient = new ZooKeeperClientImpl(zooKeeperServerRunner.connectString(), ZK_SESSION_TIMEOUT)) {
            ZooKeeperCli.Create.createCluster(zkClient, CLUSTER_ROOT_ZNODE, CLUSTER_NAME, DemoConst.NUM_PARTITIONS);
            ZooKeeperCli.Create.createStores(zkClient, CLUSTER_ROOT_ZNODE, DemoConst.NUM_PARTITIONS, storageGroupMap, storageConnectionMap);

            waltzStorageRunners = new WaltzStorageRunner[storageGroupMap.size()];
            storeParams = new StoreMetadata(zkClient, new ZNode(CLUSTER_ROOT_ZNODE, StoreMetadata.STORE_ZNODE_NAME)).getStoreParams();
        }

        // Set waltzStorageConfig
        Properties storageProps = new Properties();
        storageProps.setProperty(WaltzStorageConfig.STORAGE_JETTY_PORT, String.valueOf(storageJettyPort));
        storageProps.setProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD, String.valueOf(segmentSizeThreshold));
        storageProps.setProperty(WaltzStorageConfig.STORAGE_DIRECTORY, storageDir.toString());
        storageProps.setProperty(WaltzStorageConfig.ZOOKEEPER_CONNECT_STRING, zooKeeperServerRunner.connectString());
        storageProps.setProperty(WaltzStorageConfig.ZOOKEEPER_SESSION_TIMEOUT, String.valueOf(ZK_SESSION_TIMEOUT));
        storageProps.setProperty(WaltzStorageConfig.CLUSTER_ROOT, CLUSTER_ROOT_ZNODE.path);
        WaltzStorageConfig waltzStorageConfig = new WaltzStorageConfig(storageProps);

        int i = 0;
        for (String connectString : storageGroupMap.keySet()) {
            String[] component = connectString.split(":");
            String host = component[0];
            int port = Integer.parseInt(component[1]);

            waltzStorageRunners[i] = new WaltzStorageRunner(port, port + 1, waltzStorageConfig);
            waltzStorageRunners[i].startAsync();
            waltzStorageRunners[i].awaitStart();

            StorageAdminClient storageAdminClient = new StorageAdminClient(host, port + 1, ClientSSL.createInsecureContext(), storeParams.key, storeParams.numPartitions);
            storageAdminClient.open();
            for (int partitionId = 0; partitionId < storeParams.numPartitions; partitionId++) {
                storageAdminClient.setPartitionAssignment(partitionId, true, false);
            }
            storageAdminClient.close();
            i++;
        }

        waltzServerRunner = getWaltzServerRunner(serverPort);
        waltzServerRunner.startAsync();
    }

    void shutdownServers() throws Exception {
        if (waltzServerRunner != null) {
            waltzServerRunner.stop();
        }

        for (WaltzStorageRunner storageRunner : waltzStorageRunners) {
            if (storageRunner != null) {
                storageRunner.stop();
            }
        }

        if (zooKeeperServerRunner != null) {
            zooKeeperServerRunner.stop();
            zooKeeperServerRunner.clear();
        }
    }

    private WaltzServerRunner getWaltzServerRunner(int port) throws Exception {
        HashMap<Object, Object> map = new HashMap<>();
        map.put(WaltzServerConfig.CLUSTER_ROOT, CLUSTER_ROOT_ZNODE.path);
        map.put(WaltzServerConfig.SERVER_JETTY_PORT, String.valueOf(serverJettyPort));
        map.put(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING, zooKeeperServerRunner.connectString());
        map.put(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT, Integer.toString(ZK_SESSION_TIMEOUT));

        return new WaltzServerRunner(port, null, new WaltzServerConfig(map), false);
    }

    public void run() {
        try {
            startServers();

            String prompt = "\nEnter [start, stop, quit] \n";
            String[] command = prompt(prompt);

            while (command != null && processCmd(command)) {
                command = prompt(prompt);
            }

            shutdownServers();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    boolean processCmd(String... command) {
        if (command.length > 0) {
            switch (command[0]) {
                case "quit":
                    return false;

                case "start":
                    try {
                        if (waltzServerRunner == null) {
                            waltzServerRunner = getWaltzServerRunner(serverPort);
                        }
                        waltzServerRunner.startAsync();

                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    return true;

                case "stop":
                    try {
                        waltzServerRunner.stop();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        waltzServerRunner = null;
                    }
                    return true;

                default:
                    // Ignore;
                    return true;
            }
        } else {
            return true;
        }
    }

    String zkConnectString() {
        return zooKeeperServerRunner.connectString();
    }

    public static void main(String[] args) throws Exception {
        try (DemoServers servers = new DemoServers()) {
            servers.run();
        }
    }

}
