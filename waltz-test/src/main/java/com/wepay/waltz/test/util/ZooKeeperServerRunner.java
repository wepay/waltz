package com.wepay.waltz.test.util;

import com.wepay.riff.util.PortFinder;
import com.wepay.waltz.common.util.Utils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public class ZooKeeperServerRunner extends Runner<ZooKeeperServer> {

    private static final int TICK_TIME = 30000;
    private static final int NUM_CONNECTIONS = 5000;

    private final File zkDir;
    private final String connectString;

    private ServerCnxnFactory standaloneServerFactory;

    public ZooKeeperServerRunner(int port) throws IOException {
        this(port, Files.createTempDirectory("zookeeper-"));
    }

    public ZooKeeperServerRunner(int port, Path zkDir) throws IOException {
        int zkPort = port == 0 ? new PortFinder().getPort() : port;
        InetAddress inetAddress = InetAddress.getLocalHost();
        this.connectString = inetAddress.getCanonicalHostName() + ":" + zkPort;

        try {
            InetSocketAddress socketAddress = new InetSocketAddress(inetAddress, zkPort);
            this.standaloneServerFactory = ServerCnxnFactory.createFactory(socketAddress, NUM_CONNECTIONS);

        } catch (IOException ex) {
            throw new IOException("failed to create zookeeper ServerCnxnFactory: port=" + zkPort, ex);
        }
        this.zkDir = zkDir.toFile();
    }

    public String start() {
        startAsync();
        awaitStart();
        return connectString();
    }

    public void clear() {
        synchronized (this) {
            if (thread == null) {
                Utils.removeDirectory(zkDir);
            } else {
                throw new IllegalStateException("server running");
            }
        }
    }

    public String connectString() {
        return connectString;
    }

    protected ZooKeeperServer createServer() throws Exception {
        server = new ZooKeeperServer(zkDir, zkDir, TICK_TIME);
        standaloneServerFactory.startup(server);

        return server;
    }

    protected void closeServer() {
        server.shutdown(true);
        server = null;
    }

}
