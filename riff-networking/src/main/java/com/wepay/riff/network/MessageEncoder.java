package com.wepay.riff.network;

import com.wepay.riff.message.ByteBufMessageAttributeWriter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageEncoder extends MessageToByteEncoder<Message> {

    private final byte magicByte;
    private final short version;
    private final MessageCodec codec;

    public MessageEncoder(MessageCodec codec) {
        this.magicByte = codec.magicByte();
        this.version = codec.version();
        this.codec = codec;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        // Write the magic byte.
        out.writeByte(magicByte);

        // Write the version.
        out.writeShort(version);

        // Remember the current index and write the dummy length
        // The actual length will be filled in later using the index
        int index = out.writerIndex();
        out.writeInt(0);

        // Encode the message
        MessageAttributeWriter writer = new ByteBufMessageAttributeWriter(out);
        codec.encode(msg, writer);

        out.setInt(index, writer.bytesWritten());
    }

}
