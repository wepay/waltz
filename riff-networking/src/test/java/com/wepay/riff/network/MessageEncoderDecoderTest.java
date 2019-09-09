package com.wepay.riff.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.CorruptedFrameException;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessageEncoderDecoderTest {

    @Test
    public void testBasic() {
        Random rand = new Random();

        MessageEncoder encoder = new MessageEncoder(new MockMessageCodec((byte) 'A', (short) 0));
        MessageDecoder decoder = new MessageDecoder(new MockMessageCodec((byte) 'A', (short) 0));

        ByteBuf byteBuf = Unpooled.wrappedBuffer(new byte[100]);
        byteBuf.resetWriterIndex();

        String text = "mock message: " + rand.nextInt();
        MockMessage message = new MockMessage(text);
        LinkedList<Object> out = new LinkedList<>();

        // Our encoder/decoder does not use ChannelHandlerContext. So, it is OK to give null.
        encoder.encode(null, message, byteBuf);
        decoder.decode(null, byteBuf, out);

        assertFalse(out.isEmpty());
        assertTrue(out.get(0) instanceof MockMessage);
        assertEquals(MockMessage.MESSAGE_TYPE, ((MockMessage) out.get(0)).type());
        assertEquals(text, ((MockMessage) out.get(0)).message);
    }

    @Test
    public void testMagicByteMismatch() {
        Random rand = new Random();

        // ChannelHandlerContext is not used in our encoder/decoder
        MessageEncoder encoder = new MessageEncoder(new MockMessageCodec((byte) 'A', (short) 0));
        MessageDecoder decoder = new MessageDecoder(new MockMessageCodec((byte) 'B', (short) 0));

        ByteBuf byteBuf = Unpooled.wrappedBuffer(new byte[1000]);
        byteBuf.resetWriterIndex();

        String text = "mock message: " + rand.nextInt();
        MockMessage message = new MockMessage(text);
        LinkedList<Object> out = new LinkedList<>();

        byteBuf.resetWriterIndex();
        byteBuf.resetReaderIndex();
        out.clear();
        encoder.encode(null, message, byteBuf);

        try {
            decoder.decode(null, byteBuf, out);
            fail();
        } catch (CorruptedFrameException ex) {
            // OK
        }
    }

    @Test
    public void testVersionMismatch() {
        Random rand = new Random();

        // ChannelHandlerContext is not used in our encoder/decoder
        MessageEncoder encoder = new MessageEncoder(new MockMessageCodec((byte) 'A', (short) 0));
        MessageDecoder decoder = new MessageDecoder(new MockMessageCodec((byte) 'A', (short) 1));

        ByteBuf byteBuf = Unpooled.wrappedBuffer(new byte[1000]);
        byteBuf.resetWriterIndex();

        String text = "mock message: " + rand.nextInt();
        MockMessage message = new MockMessage(text);
        LinkedList<Object> out = new LinkedList<>();

        byteBuf.resetWriterIndex();
        byteBuf.resetReaderIndex();
        out.clear();
        encoder.encode(null, message, byteBuf);

        try {
            decoder.decode(null, byteBuf, out);
            fail();
        } catch (CorruptedFrameException ex) {
            // OK
        }
    }

}
