package com.wepay.waltz.store.internal.metadata;

import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConnectionMetadataSerializer extends SerializerHelper<ConnectionMetadata> {

    public static final ConnectionMetadataSerializer INSTANCE = new ConnectionMetadataSerializer();

    @Override
    public void serialize(ConnectionMetadata connectionMetadata, DataOutput out) throws IOException {
        connectionMetadata.writeTo(out);
    }

    @Override
    public ConnectionMetadata deserialize(DataInput in) throws IOException {
        return ConnectionMetadata.readFrom(in);
    }
}
