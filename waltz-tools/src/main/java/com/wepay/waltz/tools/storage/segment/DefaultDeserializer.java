package com.wepay.waltz.tools.storage.segment;

import com.wepay.waltz.client.Serializer;

public class DefaultDeserializer implements Serializer {

    public static final DefaultDeserializer INSTANCE = new DefaultDeserializer();

    @Override
    public byte[] serialize(Object data) {
        String errorMsg = String.format("serialize() in %s class should not be called", this.getClass().getSimpleName());
        throw new RuntimeException(errorMsg);
    }

    @Override
    public PrintableObject deserialize(byte[] bytes) {
        return new PrintableObject(bytes);
    }
}
