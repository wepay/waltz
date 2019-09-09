package com.wepay.riff.network;

import java.util.HashSet;

public class HelloCodec implements MessageCodec {

    public static final HelloCodec INSTANCE = new HelloCodec();

    @Override
    public byte magicByte() {
        return 'H';
    }

    @Override
    public short version() {
        return 0;
    }

    @Override
    public Message decode(MessageAttributeReader reader) {
        HashSet<Short> versions = new HashSet<>();
        int numVersions = reader.readInt();
        for (int i = 0; i < numVersions; i++) {
            versions.add(reader.readShort());
        }

        String message = reader.readString();
        return new Hello(versions, message);
    }

    @Override
    public void encode(Message message, MessageAttributeWriter writer) {
        Hello hello = (Hello) message;

        int numVersions = hello.versions.size();
        writer.writeInt(numVersions);
        for (Short version : hello.versions) {
            writer.writeShort(version);
        }

        writer.writeString(hello.message);
    }

}
