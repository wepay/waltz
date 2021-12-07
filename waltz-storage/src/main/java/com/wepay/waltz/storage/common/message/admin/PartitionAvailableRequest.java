package com.wepay.waltz.storage.common.message.admin;

import java.util.List;

/**
 * An subclass of {@link AdminMessage} that updates the partition availability of a given storage node.
 * Toggle true to allow the storage node to read and write a partition. Toggle false to
 * prevent the storage node from reading or writing a partition.
 */
public class PartitionAvailableRequest extends AdminMessage {

    public final List<Integer> partitionsIds;
    public final boolean toggled;

    public PartitionAvailableRequest(long seqNum, List<Integer> partitionIds, boolean toggled) {
        super(seqNum);

        this.partitionsIds = partitionIds;
        this.toggled = toggled;
    }

    @Override
    public byte type() {
        return AdminMessageType.PARTITION_AVAILABLE_REQUEST;
    }
}
