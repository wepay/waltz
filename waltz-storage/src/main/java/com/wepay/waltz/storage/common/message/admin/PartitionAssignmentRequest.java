package com.wepay.waltz.storage.common.message.admin;

import java.util.List;

/**
 * An subclass of {@link AdminMessage} that updates the partition ownership of a given storage node.
 * Toggle true to add a partition ownership to a storage node. Toggle false to remove a partition
 * ownership from a storage node.
 * DeleteStorageFiles specifies whether to delete the storage files within the partition (while
 * un-assigning the partition from the storage node) or not.
 */
public class PartitionAssignmentRequest extends AdminMessage {

    public final List<Integer> partitionIds;
    public final boolean toggled;
    public final boolean deleteStorageFiles;

    public PartitionAssignmentRequest(long seqNum, List<Integer> partitionIds, boolean toggled, boolean deleteStorageFiles) {
        super(seqNum);

        this.partitionIds = partitionIds;
        this.toggled = toggled;
        this.deleteStorageFiles = deleteStorageFiles;
    }

    @Override
    public byte type() {
        return AdminMessageType.PARTITION_ASSIGNMENT_REQUEST;
    }
}
