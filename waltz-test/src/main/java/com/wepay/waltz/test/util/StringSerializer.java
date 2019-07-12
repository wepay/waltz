package com.wepay.waltz.test.util;

import com.wepay.waltz.client.Serializer;

import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String>, com.wepay.zktools.zookeeper.Serializer<String> {

    public static final StringSerializer INSTANCE = new StringSerializer();

    @Override
    public byte[] serialize(String data) {
        return data.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
