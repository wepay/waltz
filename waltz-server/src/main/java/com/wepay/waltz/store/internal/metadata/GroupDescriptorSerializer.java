package com.wepay.waltz.store.internal.metadata;

import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements the methods to serialize and deserialize {@link GroupDescriptor} object.
 */
public class GroupDescriptorSerializer extends SerializerHelper<GroupDescriptor> {

    public static final GroupDescriptorSerializer INSTANCE = new GroupDescriptorSerializer();

    @Override
    public void serialize(GroupDescriptor groupDescriptor, DataOutput out) throws IOException {
        groupDescriptor.writeTo(out);
    }

    @Override
    public GroupDescriptor deserialize(DataInput in) throws IOException {
        return GroupDescriptor.readFrom(in);
    }

}
