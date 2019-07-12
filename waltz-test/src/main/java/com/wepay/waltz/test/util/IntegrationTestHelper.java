package com.wepay.waltz.test.util;

import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.storage.WaltzStorageConfig;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.store.internal.metadata.StoreMetadata;
import com.wepay.waltz.store.internal.metadata.StoreParams;
import com.wepay.waltz.tools.zk.ZooKeeperCli;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.internal.ZooKeeperClientImpl;
import io.netty.handler.ssl.SslContext;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * This an utility class that helps integration test. It is common
 * used by {@link com.wepay.waltz.common.util.Cli} related test.
 * An example of usage:
 * 1. create a {@link IntegrationTestHelper} object
 * 2. call startZooKeeperServer()
 * 3. call startWaltzStorage()
 * 4. call startWaltzServer()
 * 5. run CLI
 * 6. do assert logic
 * 7. call closeALl()
 */
public class IntegrationTestHelper {

    private static final int STORAGE_GROUP_ID = 0;

    private final String sslConfigPath;
    private final SslSetup sslSetup;
    private final Properties props;

    private ZooKeeperServerRunner zkServerRunner;
    private Map<Integer, WaltzServerRunner> waltzServerRunners; // port number -> server runner
    private Map<Integer, WaltzStorageRunner> waltzStorageRunners; // port number -> storage runner
    private Map<Integer, StorageAdminClient> storageAdminClients; // admin port number -> storageAdminClient
    private UUID key;
    private final Path workDir;
    private final String host;

    private int zkPort;
    private int storagePort;
    private int storageAdminPort;
    private int storageJettyPort;
    private int serverPort;
    private int serverJettyPort;

    private final Map<String, Integer> storageGroupMap;
    private final Map<String, Integer> storageConnectionMap;
    private final ZNode root;
    private final String zkConnectString;

    private final String znodePath;
    private final int numPartitions;
    private final int zkSessionTimeout;
    private final long segmentSizeThreshold;

    /**
     * Constructor of IntegrationTestHelper.
     * @param props It must contains keys of Config.ZNode_PATH, Config.NUM_PARTITIONS
     *              and Config.ZK_SESSION_TIMEOUT.
     * @throws Exception
     */
    public IntegrationTestHelper(Properties props) throws Exception {
        if (!props.containsKey(Config.ZNODE_PATH)) {
            throw new IllegalArgumentException(Config.ZNODE_PATH + " is required but missing from properties.");
        }
        if (!props.containsKey(Config.NUM_PARTITIONS)) {
            throw new IllegalArgumentException(Config.NUM_PARTITIONS + " is required but missing from properties.");
        }
        if (!props.containsKey(Config.ZK_SESSION_TIMEOUT)) {
            throw new IllegalArgumentException(Config.ZK_SESSION_TIMEOUT + " is required but missing from properties.");
        }
        if (props.containsKey(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD)) {
            segmentSizeThreshold = Integer.parseInt(props.getProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD));
        } else {
            segmentSizeThreshold = WaltzStorageConfig.DEFAULT_SEGMENT_SIZE_THRESHOLD;
        }
        this.znodePath = props.getProperty(Config.ZNODE_PATH);
        this.numPartitions = Integer.parseInt(props.getProperty(Config.NUM_PARTITIONS));
        this.zkSessionTimeout = Integer.parseInt(props.getProperty(Config.ZK_SESSION_TIMEOUT));
        this.root = new ZNode(znodePath);

        this.sslSetup = new SslSetup();

        props.setProperty(WaltzServerConfig.CLUSTER_ROOT, root.path);
        this.sslSetup.setConfigParams(props, WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
        this.props = props;

        PortFinder portFinder = new PortFinder();
        this.zkPort = portFinder.getPort();
        this.storagePort = portFinder.getPort();
        this.storageAdminPort = portFinder.getPort();
        this.storageJettyPort = portFinder.getPort();
        this.serverPort = portFinder.getPort();
        this.serverJettyPort = portFinder.getPort();

        this.host = InetAddress.getLocalHost().getCanonicalHostName();
        this.zkConnectString = host + ":" + zkPort;
        this.storageGroupMap = Utils.map(host + ":" + storagePort, STORAGE_GROUP_ID);
        this.storageConnectionMap = Utils.map(host + ":" + storagePort, storageAdminPort);
        this.workDir = Files.createTempDirectory("waltz-integration-test");
        this.sslConfigPath = createYamlConfigFile(this.workDir, "ssl.yaml", props);

        this.waltzServerRunners = new HashMap<>();
        this.waltzStorageRunners = new HashMap<>();
        this.storageAdminClients = new HashMap<>();
    }

    /**
     * An utility function to help create yml config file.
     * @param dir directory which the ssl yaml config file is in
     * @param fileName name of the yaml config file
     * @param prefix ssl config prefix
     * @param sslSetup an instance of SslSetup for testing
     * @return path of the newly created yaml config file
     * @throws IOException
     */
    public static String createYamlConfigFile(Path dir, String fileName, String prefix, SslSetup sslSetup) throws IOException {
        Properties props = new Properties();
        sslSetup.setConfigParams(props, prefix);
        return createYamlConfigFile(dir, fileName, props);
    }

    /**
     * An utility function to help create yml config file.
     * @param dirName
     * @param fileName
     * @param props
     * @return
     * @throws IOException
     */
    public static String createYamlConfigFile(String dirName, String fileName, Properties props) throws IOException {
        return createYamlConfigFile(Files.createTempDirectory(dirName), fileName, props);
    }

    private static String createYamlConfigFile(Path dir, String fileName, Properties props) throws IOException {
        Yaml yaml = new Yaml();
        String filePath = dir.resolve(fileName).toString();

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, true), StandardCharsets.UTF_8))) {
            bw.write(yaml.dump(props));
            return filePath;
        }
    }

    public void startZooKeeperServer() throws Exception {
        if (zkServerRunner == null) {
            zkServerRunner = getZooKeeperServerRunner();
        }
        zkServerRunner.start();

        ZooKeeperClient zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout);
        try {
            ZooKeeperCli.Create.createCluster(zkClient, root, "test cluster", numPartitions);
            ZooKeeperCli.Create.createStores(zkClient, root, numPartitions, storageGroupMap, storageConnectionMap);

            // Set cluster key
            key = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME)).getStoreParams().key;

        } finally {
            zkClient.close();
        }
    }

    public void startWaltzServer(boolean awaitStart) throws Exception {
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(serverPort, serverJettyPort);
        waltzServerRunner.startAsync();
        if (awaitStart) {
            waltzServerRunner.awaitStart();
        }
    }

    public void startWaltzStorage(boolean awaitStart) throws Exception {
        WaltzStorageRunner waltzStorageRunner = getWaltzStorageRunner();
        waltzStorageRunner.startAsync();
        if (awaitStart) {
            waltzStorageRunner.awaitStart();
        }
    }

    /**
     * Sets the cluster key.
     *
     * @return UUID key
     */
    public void setClusterKey(UUID clusterKey) {
        key = clusterKey;
    }

    /**
     * Return cluster key. Must call startZooKeeperServer() or setClusterKey() first to set cluster key.
     *
     * @return UUID key
     */
    public UUID getClusterKey() throws IllegalAccessException {
        if (key == null) {
            throw new IllegalAccessException("zookeeper cluster has not been created");
        }
        return key;
    }

    /**
     * Return ZooKeeperServerRunner with auto-assigned port
     */
    public ZooKeeperServerRunner getZooKeeperServerRunner() throws Exception {
        return new ZooKeeperServerRunner(zkPort, workDir.resolve("zookeeper"));
    }

    /**
     * Return WaltzServerRunner with auto-assigned serverPort and serverJettyPort
     */
    public WaltzServerRunner getWaltzServerRunner() throws Exception {
        return getWaltzServerRunner(serverPort, serverJettyPort);
    }

    /**
     * Return WaltzServerRunner with custom serverPort and serverJettyPort
     */
    public WaltzServerRunner getWaltzServerRunner(int port, int jettyPort) throws Exception {
        synchronized (waltzServerRunners) {
            WaltzServerRunner waltzServerRunner = waltzServerRunners.get(port);
            if (waltzServerRunner == null) {
                Map<Object, Object> config = new HashMap<>(props);
                config.put(WaltzServerConfig.ZOOKEEPER_CONNECT_STRING, zkConnectString);
                config.put(WaltzServerConfig.ZOOKEEPER_SESSION_TIMEOUT, zkSessionTimeout);
                if (jettyPort > 0) {
                    config.put(WaltzServerConfig.SERVER_JETTY_PORT, jettyPort);
                }
                waltzServerRunner = new WaltzServerRunner(port, null, new WaltzServerConfig(config), false);
                waltzServerRunners.put(port, waltzServerRunner);
            }
            return waltzServerRunner;
        }
    }

    /**
     * Return WaltzStorageRunner with auto-assigned serverPort and storageJettyPort
     */
    public WaltzStorageRunner getWaltzStorageRunner() throws Exception {
        return getWaltzStorageRunner(storagePort, storageAdminPort, storageJettyPort);
    }

    /**
     * Return WaltzStorageRunner with custom storagePort and storageJettyPort
     */
    public WaltzStorageRunner getWaltzStorageRunner(int port, int adminPort, int jettyPort) throws Exception {
        synchronized (waltzStorageRunners) {
            WaltzStorageRunner waltzStorageRunner = waltzStorageRunners.get(port);
            if (waltzStorageRunner == null) {
                // Create a storage directory under the work dir is missing.
                Path storageDir = workDir.resolve("storage-" + port);
                if (!Files.exists(storageDir)) {
                    Files.createDirectory(storageDir);
                }

                Properties props = new Properties();
                props.setProperty(WaltzStorageConfig.STORAGE_JETTY_PORT, String.valueOf(jettyPort));
                props.setProperty(WaltzStorageConfig.STORAGE_DIRECTORY, storageDir.toString());
                props.setProperty(WaltzStorageConfig.SEGMENT_SIZE_THRESHOLD, Long.toString(segmentSizeThreshold));
                props.setProperty(WaltzStorageConfig.CLUSTER_NUM_PARTITIONS, String.valueOf(numPartitions));
                props.setProperty(WaltzStorageConfig.CLUSTER_KEY, String.valueOf(key));
                sslSetup.setConfigParams(props, WaltzStorageConfig.STORAGE_SSL_CONFIG_PREFIX);

                waltzStorageRunner = new WaltzStorageRunner(port, adminPort, new WaltzStorageConfig(props));
                waltzStorageRunners.put(port, waltzStorageRunner);
            }
            return waltzStorageRunner;
        }
    }

    /**
     * Return an existing StorageAdminClient if there is one, otherwise a new one is created and opened.
     * The Storage client requires ZooKeeperServer to be running, meaning you must call startZooKeeperServer()
     * before calling this method.
     *
     * @return StorageAdminClient instance
     * @throws Exception
     */
    public StorageAdminClient getStorageAdminClient() throws Exception {
        return getStorageAdminClient(storageAdminPort);
    }

    /**
     * Return a new instance of StorageAdminClient.
     * The Storage client requires ZooKeeperServer to be running, meaning you must call startZooKeeperServer()
     * before calling this method.
     *
     * @return StorageAdminClient instance
     * @throws Exception
     */
    public StorageAdminClient getStorageAdminClient(int adminPort) throws Exception {
        synchronized (storageAdminClients) {
            StorageAdminClient storageAdminClient = storageAdminClients.get(adminPort);
            if (storageAdminClient == null) {
                ZooKeeperClient zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout);
                StoreParams storeParams = new StoreMetadata(zkClient, new ZNode(root, StoreMetadata.STORE_ZNODE_NAME)).getStoreParams();

                storageAdminClient = new StorageAdminClient(host, adminPort, getSslContext(), storeParams.key, storeParams.numPartitions);
                storageAdminClient.open();
                storageAdminClients.put(adminPort, storageAdminClient);
            }

            return storageAdminClient;
        }
    }

    /**
     * This method assigns/unassigns all partitions to the storage node via the Storage admin client
     */
    public void setWaltzStorageAssignment(boolean isAssigned) throws Exception {
        setWaltzStorageAssignment(storageAdminPort, isAssigned);
    }

    /**
     * This method assigns/unassigns all partitions to the storage node via the Storage admin client
     */
    public void setWaltzStorageAssignment(int adminPort, boolean isAssigned) throws Exception {
        StorageAdminClient storageAdminClient = getStorageAdminClient(adminPort);

        List<Future<?>> futures = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            futures.add(storageAdminClient.setPartitionAssignment(i, isAssigned, isAssigned ? false : true));
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    public void closeAll() throws Exception {
        closeStorageAdminClient();
        shutdownWaltzServer();
        shutdownWaltzStorage();
        cleanupZnode();
        shutdownZooKeeperServer();
        cleanupSSL();
        cleanupWorkDir();
    }

    private void shutdownWaltzServer() throws Exception {
        for (WaltzServerRunner waltzServerRunner : waltzServerRunners.values()) {
            waltzServerRunner.stop();
        }
    }

    private void shutdownWaltzStorage() throws Exception {
        for (WaltzStorageRunner waltzStorageRunner : waltzStorageRunners.values()) {
            waltzStorageRunner.stop();
        }
    }

    private void shutdownZooKeeperServer() throws Exception {
        if (zkServerRunner != null) {
            zkServerRunner.stop();
        }
    }

    private void cleanupZnode() throws Exception {
        if (zkServerRunner != null) {
            ZooKeeperClient zkClient = new ZooKeeperClientImpl(zkConnectString, zkSessionTimeout);
            try {
                zkClient.deleteRecursively(root);
            } finally {
                zkClient.close();
            }
        }
    }

    private void cleanupSSL() {
        sslSetup.close();
    }

    private void cleanupWorkDir() {
        Utils.removeDirectory(workDir.toFile());
    }

    private void closeStorageAdminClient() {
        for (StorageAdminClient storageAdminClient : storageAdminClients.values()) {
            storageAdminClient.close();
        }
    }

    public String getHost() {
        return host;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getStoragePort() {
        return storagePort;
    }

    public String getStorageConnectString() {
        return host + ":" + storagePort;
    }

    public int getStorageAdminPort() {
        return storageAdminPort;
    }

    public int getStorageJettyPort() {
        return storageJettyPort;
    }

    public int getServerPort() {
        return serverPort;
    }

    public int getServerJettyPort() {
        return serverJettyPort;
    }

    public String getZkConnectString() {
        return zkConnectString;
    }

    public String getZnodePath() {
        return znodePath;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public long getSegmentSizeThreshold() {
        return segmentSizeThreshold;
    }

    public int getZkSessionTimeout() {
        return zkSessionTimeout;
    }

    public String getSslConfigPath() {
        return sslConfigPath;
    }

    public SslSetup getSslSetup() {
        return sslSetup;
    }

    public SslContext getSslContext() throws GeneralSecurityException, IOException {
        return Utils.getSslContext(sslConfigPath, WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
    }

    public Path getWorkDir() {
        return workDir;
    }

    public static class Config {
        public static final String NUM_PARTITIONS = "numPartitions";
        public static final String ZK_SESSION_TIMEOUT = "zookeeper.sessionTimeout";
        public static final String ZNODE_PATH = "zookeeper.znodePath";
    }

}
