package com.wepay.waltz.test.smoketest;

import com.wepay.waltz.client.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class TxnSerializer implements Serializer<UUID> {

    public static final TxnSerializer INSTANCE = new TxnSerializer();

    @Override
    public byte[] serialize(UUID uuid) {
        return uuid.toString().getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public UUID deserialize(byte[] bytes) {
        return UUID.fromString(new String(bytes, StandardCharsets.US_ASCII));
    }

}
