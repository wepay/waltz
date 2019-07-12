package com.wepay.waltz.storage.common.message.admin;

/**
 * An subclass of {@AdminMessage} that updates the partition availability of a given storage node.
 * Toggle true to allow the storage node to read and write a partition. Toggle false to
 * prevent the storage node from reading or writing a partition.
 */
public class PartitionAvailableRequest extends AdminMessage {

    public final int partitionId;
    public final boolean toggled;

    public PartitionAvailableRequest(long seqNum, int partitionId, boolean toggled) {
        super(seqNum);

        this.partitionId = partitionId;
        this.toggled = toggled;
    }

    @Override
    public byte type() {
        return AdminMessageType.PARTITION_AVAILABLE_REQUEST;
    }
}
