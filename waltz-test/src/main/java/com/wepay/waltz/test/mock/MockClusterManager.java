package com.wepay.waltz.test.mock;

import com.wepay.zktools.clustermgr.ClusterManager;
import com.wepay.zktools.clustermgr.ClusterManagerException;
import com.wepay.zktools.clustermgr.Endpoint;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.internal.PartitionAssignment;
import com.wepay.zktools.clustermgr.internal.ServerDescriptor;
import com.wepay.zktools.zookeeper.ZNode;

import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MockClusterManager implements ClusterManager {

    private static final String CLUSTER_NAME = "mock cluster";

    private final int numPartitions;

    private final LinkedList<ManagedClient> managedClients = new LinkedList<>();
    private final IdentityHashMap<ManagedServer, ZNode> managedServers = new IdentityHashMap<>();
    private final HashMap<ZNode, ServerDescriptor> serverDescriptors = new HashMap<>();

    private final SecureRandom rnd = new SecureRandom();
    private AtomicInteger serverId = new AtomicInteger(rnd.nextInt());
    private AtomicInteger clientId = new AtomicInteger(rnd.nextInt());

    public MockClusterManager(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public void close() {

    }

    @Override
    public String clusterName() {
        return CLUSTER_NAME;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public Set<ServerDescriptor> serverDescriptors() throws ClusterManagerException {
        return new HashSet<>(serverDescriptors.values());
    }

    @Override
    public PartitionAssignment partitionAssignment() throws ClusterManagerException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + "does not assign partitions");
    }

    @Override
    public void manage(ManagedClient client) throws ClusterManagerException {
        synchronized (managedClients) {
            client.setClientId(clientId.incrementAndGet());
            managedClients.add(client);
        }
    }

    @Override
    public void manage(ManagedServer server) throws ClusterManagerException {
        synchronized (managedServers) {
            if (managedServers.containsKey(server)) {
                throw new ClusterManagerException("server already managed");
            }

            int id = serverId.incrementAndGet();
            try {
                // Fake znode
                ZNode znode = new ZNode("/serverDescriptors/s_" + id);
                managedServers.put(server, znode);
                serverDescriptors.put(znode, new ServerDescriptor(id, server.endpoint(), Collections.emptyList()));
                server.setServerId(id);
            } catch (Exception ex) {
                throw new ClusterManagerException("unable to manage server", ex);
            }
        }
    }

    @Override
    public void unmanage(ManagedClient client) throws ClusterManagerException {
        synchronized (managedClients) {
            managedClients.remove(client);
        }
    }

    @Override
    public void unmanage(ManagedServer server) throws ClusterManagerException {
        synchronized (managedServers) {
            ZNode znode = managedServers.remove(server);
            if (znode != null) {
                serverDescriptors.remove(znode);
            }
        }
    }

    public Collection<ManagedClient> managedClients() {
        return Collections.unmodifiableList(managedClients);
    }

    public Collection<ManagedServer> managedServers() {
        return Collections.unmodifiableSet(managedServers.keySet());
    }

    public Endpoint endPoint(ManagedServer server) {
        synchronized (managedServers) {
            ServerDescriptor descriptor = serverDescriptors.get(managedServers.get(server));
            return descriptor != null ? descriptor.endpoint : null;
        }
    }

}
