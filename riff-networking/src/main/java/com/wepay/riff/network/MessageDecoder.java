package com.wepay.riff.network;

import com.wepay.riff.message.ByteBufMessageAttributeReader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {

    private final byte magicByte;
    private final short version;
    private final MessageCodec codec;

    public MessageDecoder(MessageCodec codec) {
        this.magicByte = codec.magicByte();
        this.version = codec.version();
        this.codec = codec;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Wait until the magic byte, version, and the length prefix is available.
        if (in.readableBytes() < 7) {
            return;
        }

        in.markReaderIndex();

        // Check the magic byte.
        byte magicByte = in.readByte();
        if (magicByte != this.magicByte) {
            byte[] bytes = getFirstBytes(in, 32);
            String txt = printableString(bytes);
            String hex = hexString(bytes);

            throw new CorruptedFrameException(
                "Invalid Message: magicByte=[expected=" + this.magicByte + " actual=" + magicByte + "]"
                    + " channel=" + ctx + " txt=[" + txt + "] hex=[" + hex + "]");
        }

        // Check the version
        short version = in.readShort();
        if (version != this.version) {
            byte[] bytes = getFirstBytes(in, 32);
            String txt = printableString(bytes);
            String hex = hexString(bytes);

            throw new CorruptedFrameException(
                "Invalid Message: magicByte=" + magicByte
                    + " version[expected=" + this.version + " actual=" + version + "]"
                    + " channel=" + ctx + " txt=[" + txt + "] hex=[" + hex + "]");
        }

        // Wait until the whole data is available.
        int length = in.readInt();
        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        // Decode the received data into a new Message.
        MessageAttributeReader reader = new ByteBufMessageAttributeReader(in, length);
        out.add(codec.decode(reader));

        reader.ensureReadCompletely();
    }

    private byte[] getFirstBytes(ByteBuf in, int maxBytes) {
        in.resetReaderIndex();
        int numBytes = Math.min(maxBytes, in.readableBytes());
        byte[] bytes = new byte[numBytes];
        in.readBytes(bytes);
        in.resetReaderIndex();

        return bytes;
    }

    private String printableString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {
            char c = (char) (b & 0xFF);
            if (!Character.isISOControl(c)) {
                sb.append(c);
            } else {
                sb.append('.');
            }
        }

        return sb.toString();
    }

    private String hexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }

}
