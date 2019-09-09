package com.wepay.riff.message;

import com.wepay.riff.network.MessageAttributeWriter;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class ByteBufMessageAttributeWriter extends MessageAttributeWriter {

    private final ByteBuf buf;
    private int bytesWritten;

    public ByteBufMessageAttributeWriter(ByteBuf buf) {
        this.buf = buf;
        this.bytesWritten = 0;
    }

    public void writeByte(byte b) {
        buf.writeByte(b);
        bytesWritten += 1;
    }

    public void writeShort(short s) {
        buf.writeShort(s);
        bytesWritten += 2;
    }

    public void writeInt(int v) {
        buf.writeInt(v);
        bytesWritten += 4;
    }

    public void writeLong(long v) {
        buf.writeLong(v);
        bytesWritten += 8;
    }

    public void writeDouble(double v) {
        buf.writeDouble(v);
        bytesWritten += 8;
    }

    public void writeByteArray(byte[] array) {
        if (array != null) {
            buf.writeInt(array.length);
            buf.writeBytes(array);
            bytesWritten += (4 + array.length);

        } else {
            buf.writeInt(-1);
            bytesWritten += 4;
        }
    }

    public void writeShortArray(short[] array) {
        if (array != null) {
            buf.writeInt(array.length);
            for (short value : array) {
                buf.writeShort(value);
            }
            bytesWritten += (4 + array.length * 2);

        } else {
            buf.writeInt(-1);
            bytesWritten += 4;
        }
    }

    public void writeIntArray(int[] array) {
        if (array != null) {
            buf.writeInt(array.length);
            for (int value : array) {
                buf.writeInt(value);
            }
            bytesWritten += (4 + array.length * 4);

        } else {
            buf.writeInt(-1);
            bytesWritten += 4;
        }
    }

    public void writeBoolean(boolean b) {
        writeByte(b ? (byte) 1 : (byte) 0);
    }

    public void writeString(String text) {
        byte[] array = text != null ? text.getBytes(StandardCharsets.UTF_8) : null;
        writeByteArray(array);
    }

    public int bytesWritten() {
        return bytesWritten;
    }

}
