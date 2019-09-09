package com.wepay.riff.network;

public interface MessageCodec {

    short version();

    byte magicByte();

    Message decode(MessageAttributeReader reader);

    void encode(Message message, MessageAttributeWriter writer);

}
