package com.wepay.waltz.common.metadata;

import com.wepay.waltz.exception.StoreMetadataException;
import com.wepay.zktools.zookeeper.Handlers.OnNodeChanged;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.WatcherHandle;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperClient;
import com.wepay.zktools.zookeeper.ZooKeeperClientException;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class implements the store metadata to keep track of the state of the storage nodes.
 */
public class StoreMetadata {

    public static final String STORE_ZNODE_NAME = "store";
    public static final String PARTITION_ZNODE_NAME = "partition";
    public static final String ASSIGNMENT_ZNODE_NAME = "assignment";
    public static final String GROUP_ZNODE_NAME = "group";
    public static final String CONNECTION_ZNODE_NAME = "connection";

    private static final String ADMIN_LOCK_ZNODE_NAME = "adminLock";

    private final ZooKeeperClient zkClient;
    private final ZNode storeRoot;

    /**
     * Class constructor.
     * @param zkClient The ZooKeeperClient used for Waltz Cluster.
     * @param storeRoot Path to the store ZNode.
     */
    public StoreMetadata(ZooKeeperClient zkClient, ZNode storeRoot) {
        this.zkClient = zkClient;
        this.storeRoot = storeRoot;
    }

    /**
     * Creates store, partition, group, assignment, connection ZNode.
     * @param numPartitions The number of partitions.
     * @param storageGroups A map of (storage_node_connect_string, group_id).
     * @param connectionMetadata Map of Storage connect string (in host:port format)
     *                           to its corresponding admin port.
     * @throws Exception is thrown if the initialization fails.
     */
    public void create(int numPartitions, Map<String, Integer> storageGroups, Map<String, Integer> connectionMetadata) throws Exception {
        zkClient.createPath(storeRoot);
        zkClient.createPath(new ZNode(storeRoot, PARTITION_ZNODE_NAME));
        zkClient.createPath(new ZNode(storeRoot, GROUP_ZNODE_NAME));
        zkClient.createPath(new ZNode(storeRoot, ASSIGNMENT_ZNODE_NAME));
        zkClient.createPath(new ZNode(storeRoot, CONNECTION_ZNODE_NAME));

        initStoreParams(numPartitions);
        initGroupDescriptor(storageGroups);
        initReplicaAssignments(numPartitions, storageGroups);
        initConnectionMetadata(connectionMetadata);
    }

    /**
     * Initializes a store root znode in Zookeeper. If the ZNode has already been initialized,
     * throw IllegalStateException. Otherwise, this sets store parameters (the cluster key, the number of partitions).
     * @param numPartitions the number of partitions
     * @throws Exception
     */
    private void initStoreParams(int numPartitions) throws Exception {
        UUID key = UUID.randomUUID();

        mutex(session -> {
            NodeData<StoreParams> nodeData = session.getStoreParamsNodeData();

            if (nodeData.value != null) {
                throw new IllegalStateException("The store root has already been set.");
            }

            session.setStoreParams(new StoreParams(key, numPartitions));
        });
    }

    /**
     * Initializes a group descriptor znode in Zookeeper. If the znode has already been initialized,
     * throw IllegalStateException. If storageGroups is null, throw NullPointerException.
     * Otherwise, this sets a new group descriptor data.
     * @param storageGroups a map of <storage_node_connect_string, group_id>
     * @throws Exception
     */
    private void initGroupDescriptor(Map<String, Integer> storageGroups) throws Exception {
        if (storageGroups == null) {
            throw new NullPointerException("storageGroups must not be null");
        }

        mutex(session -> {
            NodeData<GroupDescriptor> nodeData = session.getGroupDescriptorNodeData();

            if (nodeData.value != null) {
                throw new IllegalStateException("The group descriptor Znode has already been set.");
            }

            session.setGroupDescriptor(new GroupDescriptor(storageGroups), -1);
        });
    }
    /**
     * Initialize a replica assignments znode in Zookeeper. If the znode has already been initialized,
     * throw IllegalStateException. If storageGroups is null, throw NullPointerException.
     * Otherwise, this sets a new replica assignment data.
     * @param numPartitions number of partitions
     * @param storageGroups a map of <storage_node_connect_string, group_id>
     * @throws Exception
     */
    private void initReplicaAssignments(int numPartitions, Map<String, Integer> storageGroups) throws Exception {
        if (storageGroups == null) {
            throw new NullPointerException("storageGroups must not be null");
        }

        Map<String, int[]> assignments = assignPartitions(storageGroups.keySet(), numPartitions);

        mutex(session -> {
            NodeData<ReplicaAssignments> nodeData = session.getReplicaAssignmentsNodeData();

            if (nodeData.value != null) {
                throw new IllegalStateException("The replica assignment Znode has already been set.");
            }

            session.setReplicaAssignments(new ReplicaAssignments(assignments), -1);
        });
    }

    /**
     * Initialize the connection metadata znode in Zookeeper. If the znode has already been initialized,
     * throw IllegalStateException. If connections is null, throw NullPointerException. Otherwise,
     * this sets a new connection metadata.
     * @param connections a map of <storage_node_connect_string, admin_port>
     * @throws Exception
     */
    private void initConnectionMetadata(Map<String, Integer> connections) throws Exception {
        if (connections == null) {
            throw new NullPointerException("connections must not be null");
        }

        mutex(session -> {
            NodeData<ConnectionMetadata> nodeData = session.getConnectionMetadataNodeData();

            if (nodeData.value != null) {
                throw new IllegalStateException("The connection metadata Znode has already been set.");
            }

            session.setConnectionMetadata(new ConnectionMetadata(connections), -1);
        });
    }

    /**
     * Returns the store parameters for the store.
     * @return the store parameters for the store.
     * @throws StoreMetadataException
     */
    public StoreParams getStoreParams() throws StoreMetadataException {
        try {
            return zkClient.getData(storeRoot, StoreParamsSerializer.INSTANCE).value;
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get store params", ex);
        }
    }

    /**
     * Returns the replica assignments for the store.
     * @return the replica assignments for the store.
     * @throws StoreMetadataException
     */
    public ReplicaAssignments getReplicaAssignments() throws StoreMetadataException {
        try {
            return zkClient.getData(new ZNode(storeRoot, ASSIGNMENT_ZNODE_NAME), ReplicaAssignmentsSerializer.INSTANCE).value;
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get replica assignments", ex);
        }
    }

    /**
     * Returns the group descriptor for the store.
     * @return the group descriptor for the store.
     * @throws StoreMetadataException
     */
    public GroupDescriptor getGroupDescriptor() throws StoreMetadataException {
        try {
            return zkClient.getData(new ZNode(storeRoot, GROUP_ZNODE_NAME), GroupDescriptorSerializer.INSTANCE).value;
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get group descriptor", ex);
        }
    }

    /**
     * Returns the connection metadata for the store.
     * @return the connection metadata for the store.
     * @throws StoreMetadataException
     */
    public ConnectionMetadata getConnectionMetadata() throws StoreMetadataException {
        try {
            return zkClient.getData(new ZNode(storeRoot, CONNECTION_ZNODE_NAME), ConnectionMetadataSerializer.INSTANCE).value;
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get connection metadata", ex);
        }
    }

    /**
     * Sets an replica assignment watcher
     * @param handler OnNodeChanged handler
     * @return WatcherHandle
     */
    public WatcherHandle watchReplicaAssignments(OnNodeChanged<ReplicaAssignments> handler) {
        ZNode assignmentNode = new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME);
        return zkClient.watch(assignmentNode, handler, ReplicaAssignmentsSerializer.INSTANCE);
    }

    public void mutex(StoreMetadataMutexAction action) throws KeeperException, ZooKeeperClientException, StoreMetadataException {
        zkClient.mutex(
            new ZNode(storeRoot, ADMIN_LOCK_ZNODE_NAME),
            zkSession -> action.apply(new StoreMetadataMutexSession(storeRoot, zkSession))
        );
    }

    /**
     * Add a storage node to a group. This is a non-transactional operation. If the storage
     * node already exists, throw IllegalStateException. Otherwise, update group descriptor
     * Znode first. And then, update replica assignments Znode.
     * @param storage storage node connect string
     * @param groupId group to be assigned to
     * @param adminPort storage node admin port
     * @throws StoreMetadataException
     */
    public void addStorageNode(String storage, int groupId, int adminPort) throws KeeperException, ZooKeeperClientException, StoreMetadataException {
        mutex(session -> {
            final NodeData<GroupDescriptor> groupNodeData = session.getGroupDescriptorNodeData();
            Map<String, Integer> groups = groupNodeData.value.groups;

            final NodeData<ReplicaAssignments> assignmentNodeData = session.getReplicaAssignmentsNodeData();
            Map<String, int[]> replicas = assignmentNodeData.value.replicas;

            final NodeData<ConnectionMetadata> connectionMetadataNodeData = session.getConnectionMetadataNodeData();
            Map<String, Integer> connections = connectionMetadataNodeData.value.connections;

            // check if the storage node already exists
            if (groups.containsKey(storage) || replicas.containsKey(storage) || connections.containsKey(storage)) {
                throw new IllegalStateException("Storage node already exists.");
            }

            // add storage node to newGroups
            Map<String, Integer> newGroups = new HashMap<>(groups);
            newGroups.put(storage, groupId);

            // add storage node to newReplicas
            Map<String, int[]> newReplicas = new HashMap<>(replicas);
            newReplicas.put(storage, new int[0]);

            // add storage node to newConnections
            Map<String, Integer> newConnections = new HashMap<>(connections);
            newConnections.put(storage, adminPort);

            // update group descriptor Znode
            session.setGroupDescriptor(new GroupDescriptor(newGroups), groupNodeData.stat.getVersion());

            // update replica assignments Znode
            session.setReplicaAssignments(new ReplicaAssignments(newReplicas), assignmentNodeData.stat.getVersion());

            // update connection metadata Znode
            session.setConnectionMetadata(new ConnectionMetadata(newConnections), connectionMetadataNodeData.stat.getVersion());
        });
    }

    /**
     * Removes a storage node from group. This is a non-transactional operation. If the
     * storage node does not exists, or if any partition assigned to current storage node,
     * throw IllegalStateException. Otherwise, update replica assignments Znode first. And
     * then, update group descriptor Znode.
     * @param storage storage node connect string
     * @throws StoreMetadataException
     */
    public void removeStorageNode(String storage) throws KeeperException, ZooKeeperClientException, StoreMetadataException {
        mutex(session -> {
            final NodeData<GroupDescriptor> groupNodeData = session.getGroupDescriptorNodeData();
            Map<String, Integer> groups = groupNodeData.value.groups;

            final NodeData<ReplicaAssignments> assignmentNodeData = session.getReplicaAssignmentsNodeData();
            Map<String, int[]> replicas = assignmentNodeData.value.replicas;

            final NodeData<ConnectionMetadata> connectionMetadataNodeData = session.getConnectionMetadataNodeData();
            Map<String, Integer> connections = connectionMetadataNodeData.value.connections;

            // check if the storage node does not exist
            if (!groups.containsKey(storage) && !replicas.containsKey(storage) && !connections.containsKey(storage)) {
                throw new IllegalStateException("Storage node does not exist.");
            }

            // check if any partition assigned to the storage node
            if (replicas.get(storage) != null && replicas.get(storage).length > 0) {
                throw new IllegalStateException("Storage node cannot be removed when containing partitions");
            }

            // remove storage node from newReplicas
            Map<String, int[]> newReplicas = new HashMap<>(replicas);
            newReplicas.remove(storage);

            // remove storage node from newGroups
            Map<String, Integer> newGroups = new HashMap<>(groups);
            newGroups.remove(storage);

            // remove storage node from newConnections
            Map<String, Integer> newConnections = new HashMap<>(connections);
            newConnections.remove(storage);

            // update replica assignments Znode
            session.setReplicaAssignments(new ReplicaAssignments(newReplicas), assignmentNodeData.stat.getVersion());

            // update group descriptor Znode
            session.setGroupDescriptor(new GroupDescriptor(newGroups), groupNodeData.stat.getVersion());

            // update connection metadata Znode
            session.setConnectionMetadata(new ConnectionMetadata(newConnections), connectionMetadataNodeData.stat.getVersion());
        });
    }

    /**
     * Add given partitions to a storage node in ReplicaAssignment. If the storage node
     * does not exist, or any of the partitions does not belong to the cluster or already
     * exists in storage node, throw IllegalStateException.
     * @param partitions partitions to assign
     * @param storage storage node connect string
     * @throws StoreMetadataException
     */
    public void addPartitions(List<Integer> partitions, String storage) throws KeeperException, ZooKeeperClientException, StoreMetadataException {
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("Partitions list is empty");
        }

        mutex(session -> {
            int numPartitions = session.getStoreParamsNodeData().value.numPartitions;

            final NodeData<ReplicaAssignments> assignmentNodeData = session.getReplicaAssignmentsNodeData();
            Map<String, int[]> replicas = assignmentNodeData.value.replicas;

            Collections.sort(partitions);
            int minPartition = partitions.get(0);
            int maxPartition = partitions.get(partitions.size() - 1);

            // check if partitions belong to the cluster
            if (minPartition < 0 || maxPartition >= numPartitions) {
                throw new IllegalArgumentException(
                    String.format("One or more partition(s) in [%s, %s] does not belong to the cluster",
                        minPartition, maxPartition)
                );
            }

            Map<String, int[]> newReplicas = assignPartitions(partitions, storage, replicas);

            // update Znode with new replicaAssignments.replicas
            session.setReplicaAssignments(new ReplicaAssignments(newReplicas), assignmentNodeData.stat.getVersion());
        });
    }

    /**
     * Removes given partitions from a storage node in ReplicaAssignment. If the storage node
     * does not exist, or any of the partitions has not been assigned to the storage node, throw
     * IllegalStateException.
     * @param partitions partitions to unassign
     * @param storage storage node connect string
     * @throws KeeperException
     * @throws ZooKeeperClientException
     * @throws StoreMetadataException
     */
    public void removePartitions(List<Integer> partitions, String storage) throws KeeperException, ZooKeeperClientException, StoreMetadataException {
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("Partitions list is empty");
        }

        mutex(session -> {
            final NodeData<ReplicaAssignments> assignmentNodeData = session.getReplicaAssignmentsNodeData();
            Map<String, int[]> replicas = assignmentNodeData.value.replicas;

            final NodeData<GroupDescriptor> groupNodeData = session.getGroupDescriptorNodeData();
            Map<String, Integer> groups = groupNodeData.value.groups;

            if (!groups.containsKey(storage)) {
                throw new IllegalArgumentException(
                    String.format("Storage node %s is not part of any of the storage groups", storage)
                );
            }

            int groupToValidate = groups.get(storage);

            // create a newReplicas excluding partitions to remove
            Map<String, int[]> newReplicas = unassignPartitions(partitions, storage, replicas);

            for (Integer partitionId : partitions) {
                // validate the storage group still contains full partitions
                if (!groupContainsFullPartitions(partitionId, groupToValidate, newReplicas, groups)) {
                    throw new IllegalArgumentException(
                        String.format("Storage group does not contain full partitions after removing %s", partitionId)
                    );
                }
            }

            // update Znode with new replicaAssignments.replicas
            session.setReplicaAssignments(new ReplicaAssignments(newReplicas), assignmentNodeData.stat.getVersion());
        });
    }

    /**
     * Automatically assign all partitions to storage nodes in group. Round Robin
     * strategy will be applied. If no storage node found in the group, or any of
     * the storage node in group already has assignment, throw
     * IllegalStateException.
     * @param groupId the group contains partitions
     * @throws KeeperException
     * @throws ZooKeeperClientException
     * @throws StoreMetadataException
     */
    public void autoAssignPartition(int groupId) throws KeeperException, ZooKeeperClientException, StoreMetadataException {
        mutex(session -> {
            int numPartitions = session.getStoreParamsNodeData().value.numPartitions;

            final NodeData<ReplicaAssignments> assignmentNodeData = session.getReplicaAssignmentsNodeData();
            Map<String, int[]> replicas = assignmentNodeData.value.replicas;

            final NodeData<GroupDescriptor> groupNodeData = session.getGroupDescriptorNodeData();
            Map<String, Integer> groups = groupNodeData.value.groups;

            Set<String> storageNodesInGroup = getStorageNodesInGroup(groupId, groups);

            // check if no storage node added to group
            if (storageNodesInGroup.isEmpty()) {
                throw new IllegalStateException("No storage node found in group.");
            }

            // check if any storage nodes in group has assignment
            if ((groupHasAssignment(storageNodesInGroup, replicas))) {
                throw new IllegalStateException("Found storage node with assignment in group.");
            }

            // create a newReplicas with auto-assignment
            Map<String, int[]> newReplicas = getNewReplicas(numPartitions, storageNodesInGroup, replicas);

            // update Znode with newReplicas
            session.setReplicaAssignments(new ReplicaAssignments(newReplicas), assignmentNodeData.stat.getVersion());
        });
    }

    /**
     * Build and return an assignment map by assigning all partitions to all storage nodes.
     * @param storageServerLocations a set of storage node connect string
     * @param numPartitions number of partitions
     * @return assignments
     */
    private static Map<String, int[]> assignPartitions(Set<String> storageServerLocations, int numPartitions) {
        Map<String, int[]> assignments = new HashMap<>();
        for (String storageServerLocation : storageServerLocations) {
            int[] partitionIds = new int[numPartitions];
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                partitionIds[partitionId] = partitionId;
            }
            assignments.put(storageServerLocation, partitionIds);
        }
        return assignments;
    }

    /**
     * Return true if any storage node in group has assignment; otherwise, return false.
     * @param storageNodesInGroup a set of storage nodes in group
     * @param replicas a map of <storage_node_connect_string, partitions>
     * @return true if any storage node in group has assignment; otherwise, return false
     */
    private static boolean groupHasAssignment(Set<String> storageNodesInGroup, Map<String, int[]> replicas) {
        for (String storage: storageNodesInGroup) {
            int[] partitions = replicas.get(storage);
            if (partitions != null && partitions.length > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Auto-assign all partitions to storage nodes, and return new replicas based on that.
     * @param numPartitions number of partitions
     * @param storageNodes a set of storage nodes to assign to
     * @param oldReplicas old replica assignment, as a map of <replica, partitions>
     * @return new replicas
     */
    private static Map<String, int[]> getNewReplicas(int numPartitions, Set<String> storageNodes, Map<String, int[]> oldReplicas) {
        // add empty partition list to queue
        Queue<List<Integer>> queue = new LinkedList<>();
        for (int i = 0; i < storageNodes.size(); i++) {
            queue.add(new ArrayList<>());
        }

        // enrich each partition list with Round Robin
        for (int i = 0; i < numPartitions; i++) {
            List<Integer> partitionList = queue.poll();
            partitionList.add(i);
            queue.offer(partitionList);
        }

        // assign each partition list to a storage node
        Map<String, int[]> newReplicas = new HashMap<>(oldReplicas);
        for (String storage: storageNodes) {
            List<Integer> partitionList = queue.poll();
            int[] partitionArr = partitionList.stream().mapToInt(i -> i).toArray();
            newReplicas.put(storage, partitionArr);
        }
        return newReplicas;
    }

    /**
     * Return a set of storage nodes that belongs to the group.
     * @param groupId the group contains partitions
     * @param groups a map of <storage, group>
     * @return a set of storage nodes that belongs to the group
     */
    private static Set<String> getStorageNodesInGroup(int groupId, Map<String, Integer> groups) {
        return groups.entrySet().stream().filter(e -> e.getValue().intValue() == groupId).map(e -> e.getKey()).collect(Collectors.toSet());
    }

    /**
     * Return true if the storage group contains full partitions. Otherwise, return
     * false. This is done by checking if the partition removed is the last copy of that
     * partition among all storage nodes in the group.
     * @param removedPartition the partition removed from group
     * @param groupToValidate the group partition removed from
     * @param replicas a map of <storage, partition_array>
     * @param groups a map of <storage, group_id>
     * @return true if the group contains full partitions. Otherwise, return false.
     */
    private static boolean groupContainsFullPartitions(int removedPartition,
                                                       int groupToValidate,
                                                       Map<String, int[]> replicas,
                                                       Map<String, Integer> groups) {
        // for each storage node
        for (Map.Entry<String, Integer> entry: groups.entrySet()) {
            String storage = entry.getKey();
            int storageGroup = groups.get(storage);
            // if the storage node is in the same group as groupToValidate
            if (storageGroup == groupToValidate) {
                Set<Integer> partitions = Arrays.stream(replicas.get(storage)).boxed().collect(Collectors.toSet());
                // and if another copy of removedPartition found
                if (partitions.contains(removedPartition)) {
                    // validation pass
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Return a new replicas by copying from original replicas and adding the given partitions to the storage node.
     * @param partitionsToAssign partitions to assign
     * @param storage storage node connect string
     * @param replicas a map of <storage, partition_array>
     * @return newReplicas
     */
    private static Map<String, int[]> assignPartitions(List<Integer> partitionsToAssign,
                                                       String storage,
                                                       Map<String, int[]> replicas) {
        // check if the storage node does not exist
        if (!replicas.containsKey(storage)) {
            throw new IllegalArgumentException("Storage node does not exist.");
        }
        int[] curPartitions = replicas.get(storage);
        Set<Integer> partitions = Arrays.stream(curPartitions).boxed().collect(Collectors.toCollection(HashSet::new));

        for (Integer partitionToAssign : partitionsToAssign) {
            // check if the storage contains the partition to assign
            if (partitions.containsAll(partitionsToAssign)) {
                throw new IllegalArgumentException(
                    String.format("Partition %s already exists in the storage node.", partitionToAssign)
                );
            }
        }

        // build newPartitions
        partitions.addAll(partitionsToAssign);
        int[] newPartitions = partitions.stream().mapToInt(i -> i).toArray();
        Arrays.sort(newPartitions);

        // build newReplicas
        Map<String, int[]> newReplicas = new HashMap<>(replicas);
        newReplicas.put(storage, newPartitions);
        return newReplicas;
    }

    /**
     * Return a new replicas by copying from original replicas and removing the given partitions from
     * the storage node. If the storage node does not exist, or if the storage node does
     * not contain any of the partitions to un-assign, throw IllegalArgumentException.
     * @param partitionsToUnassign partitions to un-assign
     * @param storage storage must contain all partitions to unassign
     * @param replicas replicas must contains storage
     * @return newReplicas
     */
    private static Map<String, int[]> unassignPartitions(List<Integer> partitionsToUnassign,
                                                         String storage,
                                                         Map<String, int[]> replicas) {
        // check if the storage node does not exist
        if (!replicas.containsKey(storage)) {
            throw new IllegalArgumentException("Storage node does not exist.");
        }
        int[] curPartitions = replicas.get(storage);
        Set<Integer> partitions = Arrays.stream(curPartitions).boxed().collect(Collectors.toCollection(HashSet::new));

        for (Integer partitionToUnassign : partitionsToUnassign) {
            // check if the storage contains the partition to un-assign
            if (!partitions.contains(partitionToUnassign)) {
                throw new IllegalArgumentException(
                    String.format("Partition %s does not exist in the storage node.", partitionToUnassign)
                );
            }
        }

        // build newPartitions
        partitions.removeAll(partitionsToUnassign);
        int[] newPartitions = partitions.stream().mapToInt(i -> i).toArray();
        Arrays.sort(newPartitions);

        // build newReplicas
        Map<String, int[]> newReplicas = new HashMap<>(replicas);
        newReplicas.put(storage, newPartitions);
        return newReplicas;
    }

}
