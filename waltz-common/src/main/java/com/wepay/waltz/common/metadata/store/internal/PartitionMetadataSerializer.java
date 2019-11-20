package com.wepay.waltz.common.metadata.store.internal;

import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements the methods to serialize and deserialize {@link PartitionMetadata} object.
 */
public class PartitionMetadataSerializer extends SerializerHelper<PartitionMetadata> {

    public static final PartitionMetadataSerializer INSTANCE = new PartitionMetadataSerializer();

    @Override
    public void serialize(PartitionMetadata partitionMetadata, DataOutput out) throws IOException {
        partitionMetadata.writeTo(out);
    }

    @Override
    public PartitionMetadata deserialize(DataInput in) throws IOException {
        return PartitionMetadata.readFrom(in);
    }

}
