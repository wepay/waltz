package com.wepay.waltz.test.util;

import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.WaltzServerConfig;
import com.wepay.waltz.storage.WaltzStorageConfig;
import com.wepay.waltz.storage.client.StorageAdminClient;
import com.wepay.waltz.common.metadata.StoreMetadata;
import com.wepay.waltz.common.metadata.StoreParams;
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

    private PortFinder portFinder;
    private int zkPort;
    private List<Integer> storagePorts = new ArrayList<>();
    private List<Integer> storageAdminPorts = new ArrayList<>();
    private List<Integer> storageJettyPorts = new ArrayList<>();
    private List<Integer> serverPort = new ArrayList<>();
    private List<Integer> serverJettyPort = new ArrayList<>();

    private final Map<String, Integer> storageGroupMap;
    private final Map<String, Integer> storageConnectionMap;
    private final ZNode root;
    private final String zkConnectString;

    private final String znodePath;
    private final int numPartitions;
    private final int numStorages;
    private final int numServers;
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
        this.numStorages = Integer.parseInt(props.getProperty(Config.NUM_STORAGES, "1"));
        this.numServers = Integer.parseInt(props.getProperty(Config.NUM_SERVERS, "1"));
        this.zkSessionTimeout = Integer.parseInt(props.getProperty(Config.ZK_SESSION_TIMEOUT));
        this.root = new ZNode(znodePath);

        this.sslSetup = new SslSetup();

        props.setProperty(WaltzServerConfig.CLUSTER_ROOT, root.path);
        this.sslSetup.setConfigParams(props, WaltzServerConfig.SERVER_SSL_CONFIG_PREFIX);
        this.props = props;

        this.portFinder = new PortFinder();
        this.zkPort = portFinder.getPort();
        for  (int i = 0; i < numServers; i++) {
            this.serverPort.add(portFinder.getPort());
            this.serverJettyPort.add(portFinder.getPort());
        }

        this.host = InetAddress.getLocalHost().getCanonicalHostName();
        this.zkConnectString = host + ":" + zkPort;
        this.storageGroupMap = new HashMap<String, Integer>();
        this.storageConnectionMap = new HashMap<String, Integer>();
        for (int i = 0; i < numStorages; i++) {
            this.storagePorts.add(portFinder.getPort());
            this.storageAdminPorts.add(portFinder.getPort());
            this.storageJettyPorts.add(portFinder.getPort());
            this.storageGroupMap.put(host + ":" + storagePorts.get(i), STORAGE_GROUP_ID);
            this.storageConnectionMap.put(host + ":" + storagePorts.get(i), storageAdminPorts.get(i));
        }
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

    /**
     * Returns a port using {@link IntegrationTestHelper#portFinder} instance.
     * If in need of a port, the clients of this class should use this method
     * instead of another {@link PortFinder} instance to avoid a possible port collision.
     *
     * @return a port.
     */
    public int getPort() {
        return portFinder.getPort();
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

    /**
     * Starts the Waltz Server with serverPort[0] and serverJettyPort[0]
     * @param awaitStart If true, wait for the server to start.
     * @throws Exception if failed to start the server.
     */
    public void startWaltzServer(boolean awaitStart) throws Exception {
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner();
        waltzServerRunner.startAsync();
        if (awaitStart) {
            waltzServerRunner.awaitStart();
        }
    }

    /**
     * Starts the Waltz Server with the given server port and server jetty port.
     * @param awaitStart If true, wait for the server to start.
     * @param serverPort The Server Port.
     * @param serverJettyPort The Server Jetty port.
     * @throws Exception if failed to start the server.
     */
    public void startWaltzServer(boolean awaitStart, int serverPort, int serverJettyPort) throws Exception {
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
        return getWaltzServerRunner(0);
    }

    /**
     * Return WaltzServerRunner with auto-assigned serverPort and serverJettyPort
     */
    public WaltzServerRunner getWaltzServerRunner(int portsIndex) throws Exception {
        return getWaltzServerRunner(serverPort.get(portsIndex), serverJettyPort.get(portsIndex));
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
        return getWaltzStorageRunner(0);
    }

    /**
     * Return WaltzStorageRunner with auto-assigned serverPort and storageJettyPort
     */
    public WaltzStorageRunner getWaltzStorageRunner(int portsIndex) throws Exception {
        return getWaltzStorageRunner(storagePorts.get(portsIndex), storageAdminPorts.get(portsIndex), storageJettyPorts.get(portsIndex));
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
                props.setProperty(WaltzStorageConfig.ZOOKEEPER_CONNECT_STRING, zkConnectString);
                props.setProperty(WaltzStorageConfig.ZOOKEEPER_SESSION_TIMEOUT, String.valueOf(zkSessionTimeout));
                props.setProperty(WaltzStorageConfig.CLUSTER_ROOT, root.path);
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
        return getStorageAdminClientWithIndex(0);
    }

    /**
     * Return an existing StorageAdminClient if there is one, otherwise a new one is created and opened.
     * The Storage client requires ZooKeeperServer to be running, meaning you must call startZooKeeperServer()
     * before calling this method.
     *
     * @param adminPortIndex index of the admin port from storageAdminPorts
     * @return StorageAdminClient instance
     * @throws Exception
     */
    public StorageAdminClient getStorageAdminClientWithIndex(int adminPortIndex) throws Exception {
        return getStorageAdminClientWithAdminPort(storageAdminPorts.get(adminPortIndex));
    }

    /**
     * Return a new instance of StorageAdminClient.
     * The Storage client requires ZooKeeperServer to be running, meaning you must call startZooKeeperServer()
     * before calling this method.
     *
     * @param adminPort Admin port for the StorageAdminClient
     * @return StorageAdminClient instance
     * @throws Exception
     */
    public StorageAdminClient getStorageAdminClientWithAdminPort(int adminPort) throws Exception {
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
        setWaltzStorageAssignmentWithIndex(0, isAssigned);
    }

    /**
     * This method assigns/unassigns all partitions to the storage node via the Storage admin client
     */
    public void setWaltzStorageAssignmentWithIndex(int portIndex, boolean isAssigned) throws Exception {
        setWaltzStorageAssignmentWithPort(storageAdminPorts.get(portIndex), isAssigned);
    }

    /**
     * This method assigns/unassigns all partitions to the storage node via the Storage admin client
     */
    public void setWaltzStorageAssignmentWithPort(int adminPort, boolean isAssigned) throws Exception {
        StorageAdminClient storageAdminClient = getStorageAdminClientWithAdminPort(adminPort);

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
        return getStoragePort(0);
    }

    public int getStoragePort(int portIndex) {
        return storagePorts.get(portIndex);
    }

    public String getStorageConnectString() {
        return getStorageConnectString(0);
    }

    public String getStorageConnectString(int storagePortIndex) {
        return host + ":" + storagePorts.get(storagePortIndex);
    }

    public int getStorageAdminPort() {
        return getStorageAdminPort(0);
    }

    public int getStorageAdminPort(int portIndex) {
        return storageAdminPorts.get(portIndex);
    }

    public int getStorageJettyPort() {
        return getStorageJettyPort(0);
    }

    public int getStorageJettyPort(int portIndex) {
        return storageJettyPorts.get(portIndex);
    }

    public int getServerPort() {
        return getServerPort(0);
    }

    public int getServerPort(int portIndex) {
        return serverPort.get(portIndex);
    }

    public int getServerJettyPort() {
        return getServerJettyPort(0);
    }

    public int getServerJettyPort(int portIndex) {
        return serverJettyPort.get(portIndex);
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
        public static final String NUM_STORAGES = "numStorages";
        public static final String NUM_SERVERS = "numServers";
        public static final String ZK_SESSION_TIMEOUT = "zookeeper.sessionTimeout";
        public static final String ZNODE_PATH = "zookeeper.znodePath";
    }

}
