package com.wepay.waltz.test.smoketest;

import com.wepay.riff.metrics.core.Gauge;
import com.wepay.riff.metrics.core.MetricRegistry;
import com.wepay.riff.metrics.jmx.JmxReporter;
import com.wepay.riff.util.Logging;
import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServer;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.storage.WaltzStorage;
import com.wepay.waltz.storage.WaltzStorageConfig;
import com.wepay.waltz.store.internal.metadata.StoreMetadata;
import com.wepay.waltz.test.util.IntegrationTestHelper;
import com.wepay.waltz.test.util.Runner;
import com.wepay.waltz.test.util.RunnerScheduler;
import com.wepay.waltz.test.util.WaltzStorageRunner;
import com.wepay.waltz.tools.zk.ZooKeeperCli;
import com.wepay.zktools.util.Uninterruptibly;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SmokeTest {

    private static final Logger logger = Logging.getLogger(SmokeTest.class);
    private static final MetricRegistry REGISTRY = MetricRegistry.getInstance();

    private static final int ZK_SESSION_TIMEOUT = 30000;
    private static final String ZNODE_PATH = "/smoketest";

    private static final int STORAGE_GROUP_1 = 1;
    private static final int STORAGE_GROUP_2 = 2;
    private static final int STORAGE_GROUP_3 = 3;

    private static final int NUM_TRANSACTIONS = 1000000;
    private static final int NUM_CLIENTS = 2;
    private static final int NUM_PARTITIONS = 3;
    private static final int NUM_LOCKS = 1000;

    private static final int CLIENT_LONG_WAIT_THRESHOLD = 5000;
    private static final int CLIENT_NUM_CONSUMER_THREADS = 10;

    private static final long SEG_SIZE = 10000000;

    private static final long ZK_UP =        80000;
    private static final long ZK_DOWN =       5000;

    private static final long SERVER1_UP =   40000;
    private static final long SERVER2_UP =   60000;
    private static final long SERVER3_UP =  100000;
    private static final long SERVER_DOWN =  10000;

    private static final long STORAGE1_UP =  60000;
    private static final long STORAGE2_UP = 100000;
    private static final long STORAGE3_UP = 120000;
    private static final long STORAGE_DOWN = 10000;

    private static final double MILLIS_IN_SEC = 1000d;
    private static final double NANOS_IN_MILLIS = 1000000d;

    private static final long SLEEP = 3000;
    private static final long SLEEP_BETWEEN_TRANSACTIONS = 1;

    private static final int FREEZE_AT = -1;

    // IntegrationTestHelper allocates ZK port from the default range 10000-12000.
    // To avoid port collision, we allocate ports for server/storage from a different range.
    private static final int PORT_SEARCH_START = 12000;
    private static final int PORT_SEARCH_END = 15000;

    private final ZNode root = new ZNode(ZNODE_PATH);

    private final IntegrationTestHelper helper;

    private final WaltzClientConfig clientConfig;

    private final Map<Integer, Integer> storagePorts;
    private final Map<Integer, Integer> storageAdminPorts;
    private final Map<Integer, Integer> storageHttpPorts;

    private final Map<Integer, Integer> serverPorts;
    private final Map<Integer, Integer> serverHttpPorts;

    private final Map<String, Integer> storageGroupMap;
    private final Map<String, Integer> storageConnectionMap;

    private final int numTransactions;
    private final int numClients;
    private final int numPartitions;
    private final int numLocks;

    private RunnerScheduler<ZooKeeperServer> zooKeeperServer;
    private RunnerScheduler<WaltzStorage> waltzStorage1;
    private RunnerScheduler<WaltzStorage> waltzStorage2;
    private RunnerScheduler<WaltzStorage> waltzStorage3;
    private RunnerScheduler<WaltzServer> waltzServer1;
    private RunnerScheduler<WaltzServer> waltzServer2;
    private RunnerScheduler<WaltzServer> waltzServer3;

    private final AtomicInteger producerChecksum = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        JmxReporter.forRegistry(MetricRegistry.getInstance()).build().start();
        new SmokeTest(NUM_TRANSACTIONS, NUM_CLIENTS, NUM_PARTITIONS, NUM_LOCKS).run();
    }

    SmokeTest(int numTransactions, int numClients, int numPartitions, int numLocks) throws Exception {
        this.numTransactions = numTransactions;
        this.numClients = numClients;
        this.numPartitions = numPartitions;
        this.numLocks = numLocks;

        Properties helperConfig = new Properties();
        helperConfig.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, ZNODE_PATH);
        helperConfig.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, String.valueOf(numPartitions));
        helperConfig.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, String.valueOf(ZK_SESSION_TIMEOUT));
        helperConfig.setProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD, Long.toString(SEG_SIZE));
        helperConfig.setProperty(WaltzServerConfig.TRANSACTION_DATA_CACHE_ALLOCATION, "direct");
        this.helper = new IntegrationTestHelper(helperConfig);
        this.clientConfig = new WaltzClientConfig(Utils.map(
            WaltzClientConfig.ZOOKEEPER_CONNECT_STRING, helper.getZkConnectString(),
            WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT, String.valueOf(ZK_SESSION_TIMEOUT),
            WaltzClientConfig.CLUSTER_ROOT, ZNODE_PATH,
            WaltzClientConfig.NUM_CONSUMER_THREADS, String.valueOf(CLIENT_NUM_CONSUMER_THREADS),
            WaltzClientConfig.LONG_WAIT_THRESHOLD, String.valueOf(CLIENT_LONG_WAIT_THRESHOLD)
        ));

        PortFinder portFinder = new PortFinder(PORT_SEARCH_START, PORT_SEARCH_END);
        this.storagePorts = Utils.map(
            1, portFinder.getPort(),
            2, portFinder.getPort(),
            3, portFinder.getPort()
        );
        this.storageAdminPorts = Utils.map(
            1, portFinder.getPort(),
            2, portFinder.getPort(),
            3, portFinder.getPort()
        );
        this.storageHttpPorts = Utils.map(
            1, portFinder.getPort(),
            2, portFinder.getPort(),
            3, portFinder.getPort()
        );
        this.serverPorts = Utils.map(
            1, portFinder.getPort(),
            2, portFinder.getPort(),
            3, portFinder.getPort()
        );
        this.serverHttpPorts = Utils.map(
            1, portFinder.getPort(),
            2, portFinder.getPort(),
            3, portFinder.getPort()
        );

        this.storageGroupMap = Utils.map(
            this.helper.getHost() + ":" + this.storagePorts.get(1), STORAGE_GROUP_1,
            this.helper.getHost() + ":" + this.storagePorts.get(2), STORAGE_GROUP_2,
            this.helper.getHost() + ":" + this.storagePorts.get(3), STORAGE_GROUP_3
        );
        this.storageConnectionMap = Utils.map(
            this.helper.getHost() + ":" + this.storagePorts.get(1), this.storageAdminPorts.get(1),
            this.helper.getHost() + ":" + this.storagePorts.get(2), this.storageAdminPorts.get(2),
            this.helper.getHost() + ":" + this.storagePorts.get(3), this.storageAdminPorts.get(3)
        );
    }

    public void open() throws Exception {
        zooKeeperServer = scheduleZooKeeper(1, ZK_UP, ZK_DOWN);
        zooKeeperServer.start();

        ZooKeeperClient zkClient = new ZooKeeperClientImpl(helper.getZkConnectString(), ZK_SESSION_TIMEOUT);
        try {
            ZooKeeperCli.Create.createCluster(zkClient, root, "test cluster", numPartitions);
            ZooKeeperCli.Create.createStores(zkClient, root, numPartitions, storageGroupMap, storageConnectionMap);
            System.out.println();

            // Set cluster key
            helper.setClusterKey(
                new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME)).getStoreParams().key
            );
        } finally {
            zkClient.close();
        }
    }

    public void close() throws Exception {
        helper.closeAll();
    }

    public void createSchedulers() throws Exception {
        waltzServer1 = scheduleServer(1, SERVER1_UP, SERVER_DOWN);
        waltzServer2 = scheduleServer(2, SERVER2_UP, SERVER_DOWN);
        waltzServer3 = scheduleServer(3, SERVER3_UP, SERVER_DOWN);

        waltzStorage1 = scheduleStorage(1, STORAGE1_UP, STORAGE_DOWN);
        waltzStorage2 = scheduleStorage(2, STORAGE2_UP, STORAGE_DOWN);
        waltzStorage3 = scheduleStorage(3, STORAGE3_UP, STORAGE_DOWN);
    }

    private WaltzStorageRunner createStorageRunner(int port, int adminPort, int httpPort) throws Exception {
        WaltzStorageRunner runner = helper.getWaltzStorageRunner(port, adminPort, httpPort);

        runner.startAsync();
        runner.awaitStart();

        // Assign all partitions to this storage.
        helper.setWaltzStorageAssignmentWithPort(adminPort, true);

        runner.stop();

        return runner;
    }

    public void startWaltzServers() {
        if (waltzServer1 != null) {
            waltzServer1.start();
        }
        if (waltzServer2 != null) {
            waltzServer2.start();
        }
        if (waltzServer3 != null) {
            waltzServer3.start();
        }
    }

    public void startWaltzStorages() {
        if (waltzStorage1 != null) {
            waltzStorage1.start();
        }
        if (waltzStorage2 != null) {
            waltzStorage2.start();
        }
        if (waltzStorage3 != null) {
            waltzStorage3.start();
        }
    }

    public void shutdownWaltzServers() throws Exception {
        if (waltzServer1 != null) {
            waltzServer1.stop();
        }
        if (waltzServer2 != null) {
            waltzServer2.stop();
        }
        if (waltzServer3 != null) {
            waltzServer3.stop();
        }
    }

    public void shutdownWaltzStorages() throws Exception {
        if (waltzStorage1 != null) {
            waltzStorage1.stop();
        }
        if (waltzStorage2 != null) {
            waltzStorage2.stop();
        }
        if (waltzStorage3 != null) {
            waltzStorage3.stop();
        }
    }

    public void freeze() {
        if (zooKeeperServer != null) {
            zooKeeperServer.freeze();
        }
        if (waltzStorage1 != null) {
            waltzStorage1.freeze();
        }
        if (waltzStorage2 != null) {
            waltzStorage2.freeze();
        }
        if (waltzStorage3 != null) {
            waltzStorage3.freeze();
        }
        if (waltzServer1 != null) {
            waltzServer1.freeze();
        }
        if (waltzServer2 != null) {
            waltzServer2.freeze();
        }
        if (waltzServer3 != null) {
            waltzServer3.freeze();
        }
    }

    public void test() throws Exception {
        SmokeTestClientThread[] threads = new SmokeTestClientThread[numClients];

        for (int i = 0; i < numClients; i++) {
            int numTxns = (numTransactions / numClients) + (numTransactions % numClients > i ? 1 : 0);
            SmokeTestClientThread clientThread = new SmokeTestClientThread(
                clientConfig, numPartitions, numTxns, numLocks, SLEEP_BETWEEN_TRANSACTIONS, producerChecksum
            );
            threads[i] = clientThread;
            String metricsGroup = "smoketest.client-" + clientThread.clientId();
            REGISTRY.gauge(metricsGroup, "numProduced", (Gauge<Integer>) clientThread::numProduced);
            REGISTRY.gauge(metricsGroup, "numConsumed", (Gauge<Integer>) clientThread::numConsumed);
        }

        // Start writing
        long startTime = System.currentTimeMillis();
        long totalResponseTimeNanos = 0L;

        for (SmokeTestClientThread thread : threads) {
            thread.start();
        }

        // Wait for threads
        for (SmokeTestClientThread thread : threads) {
            Uninterruptibly.run(thread::join);
            totalResponseTimeNanos += thread.totalResponseTimeNanos();
        }

        // Close waltz clients
        Uninterruptibly.sleep(SmokeTest.SLEEP);
        for (SmokeTestClientThread thread : threads) {
            thread.closeWaltzClient();
        }

        long endTime = System.currentTimeMillis();
        double tps = (double) numTransactions / ((endTime - startTime) / MILLIS_IN_SEC);
        double latency = (double) totalResponseTimeNanos / (double) numTransactions / NANOS_IN_MILLIS;
        System.out.print("\n%\n"
            + "% WRITE elapsed[ms]=" + (endTime - startTime) + " tps=" + tps + " latency[ms]=" + latency
            + "\n%"
        );

        for (SmokeTestClientThread thread : threads) {
            thread.print();
        }
    }

    public void verifyData() throws Exception {
        System.out.println("\nStarting verification");

        long startTime = System.currentTimeMillis();

        SmokeTestClientCallbacks callbacks = new SmokeTestClientCallbacks(numPartitions, null);
        WaltzClient client = new WaltzClient(callbacks, clientConfig);

        try {
            // Wait until we've seen every message that was sent.
            while (callbacks.numConsumed() < numTransactions) {
                Uninterruptibly.sleep(SLEEP);
            }

            long endTime = System.currentTimeMillis();
            double tps = (numTransactions * numClients) / ((endTime - startTime) / MILLIS_IN_SEC);
            System.out.print("\n%\n"
                + "% VERIFICATION elapsed[ms]=" + (endTime - startTime) + " tps=" + tps
                + "\n%"
            );

            Uninterruptibly.sleep(SLEEP);

            // Validate that the checksum of everything we sent matches everything we read.
            System.out.print("\n#\n"
                + "# VERIFICATION "
                + " numConsumed=" + callbacks.numConsumed()
                + " hwmarks=" + callbacks.getClientHighWaterMarks()
                + " checksum=" + (callbacks.checksum() == producerChecksum.get() ? "OK" : "ERR")
                + "\n#"
            );
        } finally {
            try {
                client.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void verifyStorage() throws Exception {
        System.out.print("\n%\n% STORAGE VERIFICATION");

        UUID key;

        ZooKeeperClient zkClient = new ZooKeeperClientImpl(helper.getZkConnectString(), ZK_SESSION_TIMEOUT);

        try {
            key = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME)).getStoreParams().key;
        } finally {
            zkClient.close();
        }

        int[] checksums1 = ((WaltzStorageRunner) waltzStorage1.getRunner()).checksums(key, numPartitions);
        int[] checksums2 = ((WaltzStorageRunner) waltzStorage2.getRunner()).checksums(key, numPartitions);
        int[] checksums3 = ((WaltzStorageRunner) waltzStorage3.getRunner()).checksums(key, numPartitions);

        // Compare each storage node's checksums to make sure they all match as expected.
        for (int i = 0; i < numPartitions; i++) {
            boolean ok = (checksums1[i] == checksums2[i] && checksums2[i] == checksums3[i]);
            System.out.print("\n%   partition[" + i + "] " + (ok ? "OK" : "ERROR"));
        }
        System.out.println("\n%");
    }

    public void run() throws Exception {
        open();

        createSchedulers();
        startWaltzStorages();
        startWaltzServers();

        test();

        // Verify as we run.
        verifyData();

        shutdownWaltzServers();
        shutdownWaltzStorages();

        startWaltzStorages();
        startWaltzServers();

        Uninterruptibly.sleep(SLEEP);

        // Verify one more time from the beginning of the log.
        verifyData();

        shutdownWaltzServers();
        shutdownWaltzStorages();

        Uninterruptibly.sleep(SLEEP);

        verifyStorage();

        close();

        System.out.println();
    }

    private int stateNum = 0;
    private final char[] clusterState = new char[] {'.', '.', '.', ' ', '.', '.', '.', ' ', '.'};

    private void clusterStateChanged(String name, int id, char state) {
        synchronized (clusterState) {
            switch (name) {
                case "Server":
                    clusterState[id - 1] = state;
                    break;
                case "Storage":
                    clusterState[id + 3] = state;
                    break;
                case "ZooKeeper":
                    clusterState[id + 7] = state;
                    break;
                default:
                    throw new IllegalStateException("unknown server type: " + name);
            }
            ++stateNum;
            if (stateNum == FREEZE_AT) {
                freeze();
            }
            String stateString = new String(clusterState);
            logger.info(String.format("State: %04d[%s] ", stateNum, stateString));
            System.out.print(String.format("%n%04d[%s] ", stateNum, stateString));
        }
    }

    private RunnerScheduler<WaltzServer> scheduleServer(int id, long upDuration, long downDuration) throws Exception {
        int port = serverPorts.get(id);
        int httpPort = serverHttpPorts.get(id);

        String msg = String.format("Starting Server[%d]: port=%d httpPort=%d", id, port, httpPort);

        System.out.println(msg);
        Runner<WaltzServer> runner = helper.getWaltzServerRunner(port, httpPort);
        return new RunnerScheduler<WaltzServer>("Server", runner, upDuration, downDuration) {
            @Override
            protected void onServerStart() {
                clusterStateChanged(name, id, '*');
            }

            @Override
            protected void onServerStop() {
                clusterStateChanged(name, id, '.');
            }
        };
    }

    private RunnerScheduler<WaltzStorage> scheduleStorage(int id, long upDuration, long downDuration) throws Exception {
        int port = storagePorts.get(id);
        int adminPort = storageAdminPorts.get(id);
        int httpPort = storageHttpPorts.get(id);

        String msg = String.format("Starting Storage[%d]: port=%d adminPort=%d httpPort=%d", id, port, adminPort, httpPort);

        System.out.println(msg);
        Runner<WaltzStorage> runner = createStorageRunner(port, adminPort, httpPort);
        return new RunnerScheduler<WaltzStorage>("Storage", runner, upDuration, downDuration) {
            @Override
            protected void onServerStart() {
                clusterStateChanged(name, id, '*');
            }

            @Override
            protected void onServerStop() {
                clusterStateChanged(name, id, '.');
            }
        };
    }

    private RunnerScheduler<ZooKeeperServer> scheduleZooKeeper(int id, long upDuration, long downDuration) throws Exception {
        Runner<ZooKeeperServer> runner = helper.getZooKeeperServerRunner();
        return new RunnerScheduler<ZooKeeperServer>("ZooKeeper", runner, upDuration, downDuration) {
            @Override
            protected void onServerStart() {
                clusterStateChanged(name, id, '*');
            }

            @Override
            protected void onServerStop() {
                clusterStateChanged(name, id, '.');
            }
        };
    }

}
