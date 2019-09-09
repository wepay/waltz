package com.wepay.riff.message;

import com.wepay.riff.network.MessageAttributeReader;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class ByteBufMessageAttributeReader extends MessageAttributeReader {

    private final ByteBuf buf;
    private int remaining;

    public ByteBufMessageAttributeReader(ByteBuf buf, int length) {
        this.buf = buf;
        this.remaining = length;
    }

    public byte readByte() {
        checkDataSize(1);

        return buf.readByte();
    }

    public short readShort() {
        checkDataSize(2);

        return buf.readShort();
    }

    public int readInt() {
        checkDataSize(4);

        return buf.readInt();
    }

    public long readLong() {
        checkDataSize(8);

        return buf.readLong();
    }

    public double readDouble() {
        checkDataSize(8);

        return buf.readDouble();
    }

    public byte[] readByteArray() {
        checkDataSize(4);

        int arrayLength = buf.readInt();

        if (arrayLength >= 0) {
            checkDataSize(arrayLength);

            byte[] array = new byte[arrayLength];
            buf.readBytes(array);
            return array;

        } else {
            return null;
        }
    }

    public short[] readShortArray() {
        checkDataSize(4);

        int arrayLength = buf.readInt();

        if (arrayLength >= 0) {
            checkDataSize(arrayLength * 2);

            short[] array = new short[arrayLength];
            for (int i = 0; i < arrayLength; i++) {
                array[i] = buf.readShort();
            }
            return array;

        } else {
            return null;
        }
    }

    public int[] readIntArray() {
        checkDataSize(4);

        int arrayLength = buf.readInt();

        if (arrayLength >= 0) {
            checkDataSize(arrayLength * 4);

            int[] array = new int[arrayLength];
            for (int i = 0; i < arrayLength; i++) {
                array[i] = buf.readInt();
            }
            return array;

        } else {
            return null;
        }
    }

    public boolean readBoolean() {
        return readByte() == 1;
    }

    public String readString() {
        byte[] array = readByteArray();

        if (array != null) {
            return new String(array, StandardCharsets.UTF_8);

        } else {
            return null;
        }
    }

    public void ensureReadCompletely() {
        if (remaining > 0) {
            drain();
            throw new IllegalStateException("corrupted message");
        }
    }

    private void checkDataSize(int amount) {
        if (remaining < amount) {
            drain();
            throw new IllegalStateException("corrupted message");
        }
        remaining -= amount;
    }

    private void drain() {
        buf.skipBytes(remaining);
        remaining = 0;
    }

}
