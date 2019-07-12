package com.wepay.waltz.tools.performance;

import com.wepay.waltz.client.Serializer;

public class DummySerializer implements Serializer<byte[]> {

    static final DummySerializer INSTANCE = new DummySerializer();

    @Override
    public byte[] serialize(byte[] data) {
        return data;
    }

    @Override
    public byte[] deserialize(byte[] bytes) {
        return bytes;
    }
}
