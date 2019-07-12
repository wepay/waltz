package com.wepay.waltz.store.internal.metadata;

import com.wepay.zktools.zookeeper.serializer.SerializerHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StoreParamsSerializer extends SerializerHelper<StoreParams> {

    public static final StoreParamsSerializer INSTANCE = new StoreParamsSerializer();

    @Override
    public void serialize(StoreParams storeParams, DataOutput out) throws IOException {
        storeParams.writeTo(out);
    }

    @Override
    public StoreParams deserialize(DataInput in) throws IOException {
        return StoreParams.readFrom(in);
    }

}
