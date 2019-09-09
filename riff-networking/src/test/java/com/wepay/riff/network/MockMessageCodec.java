package com.wepay.riff.network;

public class MockMessageCodec implements MessageCodec {

    private final byte magicByte;
    private final short version;

    public MockMessageCodec(byte magicByte, short version) {
        this.magicByte = magicByte;
        this.version = version;
    }

    @Override
    public byte magicByte() {
        return magicByte;
    }

    @Override
    public short version() {
        return version;
    }

    @Override
    public Message decode(MessageAttributeReader reader) {
        byte messageType = reader.readByte();

        switch (messageType) {
            case MockMessage.MESSAGE_TYPE:
                String text = reader.readString();
                return new MockMessage(text);

            default:
                throw new IllegalStateException("unknown message type: " + messageType);
        }
    }

    @Override
    public void encode(Message message, MessageAttributeWriter writer) {
        writer.writeByte(message.type());

        switch (message.type()) {
            case MockMessage.MESSAGE_TYPE:
                String text = ((MockMessage) message).message;
                writer.writeString(text);
                break;

            default:
                throw new IllegalStateException("unknown message type: " + message.type());
        }
    }

}
