package com.wepay.waltz.common.metadata;

import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements the methods to serialize and deserialize {@link ReplicaAssignments} object.
 */
public class ReplicaAssignmentsSerializer extends SerializerHelper<ReplicaAssignments> {

    public static final ReplicaAssignmentsSerializer INSTANCE = new ReplicaAssignmentsSerializer();

    @Override
    public void serialize(ReplicaAssignments assignments, DataOutput out) throws IOException {
        assignments.writeTo(out);
    }

    @Override
    public ReplicaAssignments deserialize(DataInput in) throws IOException {
        return ReplicaAssignments.readFrom(in);
    }

}
