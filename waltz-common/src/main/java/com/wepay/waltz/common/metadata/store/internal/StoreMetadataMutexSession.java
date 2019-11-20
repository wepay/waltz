package com.wepay.waltz.common.metadata.store.internal;

import com.wepay.waltz.common.metadata.store.exception.StoreMetadataException;
import com.wepay.zktools.zookeeper.NodeData;
import com.wepay.zktools.zookeeper.ZNode;
import com.wepay.zktools.zookeeper.ZooKeeperSession;

/**
 * This class implements {@link StoreMetadata}'s mutex session.
 */
public class StoreMetadataMutexSession {

    private final ZNode storeRoot;
    private final ZooKeeperSession zkSession;

    /**
     * Class constructor.
     * @param storeRoot Path to the store ZNode.
     * @param zkSession The corresponding zookeeper session.
     */
    StoreMetadataMutexSession(ZNode storeRoot, ZooKeeperSession zkSession) {
        this.storeRoot = storeRoot;
        this.zkSession = zkSession;
    }

    /**
     * Returns StoreParams node data read from store Znode.
     *
     * @return StoreParams node data.
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public NodeData<StoreParams> getStoreParamsNodeData() throws StoreMetadataException {
        try {
            return zkSession.getData(storeRoot, StoreParamsSerializer.INSTANCE);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get store params", ex);
        }
    }

    /**
     * Sets StoreParams node data
     *
     * @param storeParams store parameters
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public void setStoreParams(StoreParams storeParams) throws StoreMetadataException {
        try {
            zkSession.setData(storeRoot, storeParams, StoreParamsSerializer.INSTANCE);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to set store params", ex);
        }
    }

    /**
     * Returns GroupDescriptor node data read from group Znode.
     *
     * @return GroupDescriptor node data
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public NodeData<GroupDescriptor> getGroupDescriptorNodeData() throws StoreMetadataException {
        try {
            return zkSession.getData(new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME), GroupDescriptorSerializer.INSTANCE);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get group descriptor", ex);
        }
    }

    /**
     * Sets GroupDescriptor node data.
     *
     * @param groupDescriptor The {@link GroupDescriptor} object.
     * @param version The version.
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public void setGroupDescriptor(GroupDescriptor groupDescriptor, int version) throws StoreMetadataException {
        try {
            zkSession.setData(new ZNode(storeRoot, StoreMetadata.GROUP_ZNODE_NAME), groupDescriptor, GroupDescriptorSerializer.INSTANCE, version);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to set group descriptor", ex);
        }
    }

    /**
     * Returns ReplicaAssignments node data read from assignment ZNode.
     *
     * @return ReplicaAssignments node data
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public NodeData<ReplicaAssignments> getReplicaAssignmentsNodeData() throws StoreMetadataException {
        try {
            return zkSession.getData(new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME), ReplicaAssignmentsSerializer.INSTANCE);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get replica assignments", ex);
        }
    }

    /**
     * Sets ReplicaAssignments node data.
     *
     * @param replicaAssignments The {@link ReplicaAssignments} object.
     * @param version The version.
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public void setReplicaAssignments(ReplicaAssignments replicaAssignments, int version) throws StoreMetadataException {
        try {
            zkSession.setData(new ZNode(storeRoot, StoreMetadata.ASSIGNMENT_ZNODE_NAME), replicaAssignments, ReplicaAssignmentsSerializer.INSTANCE, version);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to set replica assignments", ex);
        }
    }

    /**
     * Returns ConnectionMetadata node data read from connection ZNode.
     *
     * @return ConnectionMetadata node data
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public NodeData<ConnectionMetadata> getConnectionMetadataNodeData() throws StoreMetadataException {
        try {
            return zkSession.getData(new ZNode(storeRoot, StoreMetadata.CONNECTION_ZNODE_NAME), ConnectionMetadataSerializer.INSTANCE);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to get connection metadata", ex);
        }
    }

    /**
     * Sets ConnectionMetadata node data.
     *
     * @param connectionMetadata The {@link ConnectionMetadata} object.
     * @param version The version.
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    public void setConnectionMetadata(ConnectionMetadata connectionMetadata, int version) throws StoreMetadataException {
        try {
            zkSession.setData(new ZNode(storeRoot, StoreMetadata.CONNECTION_ZNODE_NAME), connectionMetadata, ConnectionMetadataSerializer.INSTANCE, version);
        } catch (Exception ex) {
            throw new StoreMetadataException("unable to set connection metadata", ex);
        }
    }

}
