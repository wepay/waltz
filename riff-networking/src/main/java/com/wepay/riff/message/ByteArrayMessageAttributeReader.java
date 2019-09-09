package com.wepay.riff.message;

import com.wepay.riff.network.MessageAttributeReader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ByteArrayMessageAttributeReader extends MessageAttributeReader {

    private final DataInputStream buf;

    public ByteArrayMessageAttributeReader(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public ByteArrayMessageAttributeReader(byte[] bytes, int offset, int length) {
        this.buf = new DataInputStream(new ByteArrayInputStream(bytes, offset, length));
    }

    public byte readByte() {
        try {
            return buf.readByte();

        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    public short readShort() {
        try {
            return buf.readShort();

        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    public int readInt() {
        try {
            return buf.readInt();

        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    public long readLong() {
        try {
            return buf.readLong();

        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    public double readDouble() {
        try {
            return buf.readDouble();

        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    public byte[] readByteArray() {
        try {
            int arrayLength = buf.readInt();

            if (arrayLength >= 0) {
                byte[] array = new byte[arrayLength];
                buf.readFully(array);
                return array;

            } else {
                return null;
            }
        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    public short[] readShortArray() {
        try {
            int arrayLength = buf.readInt();

            if (arrayLength >= 0) {
                short[] array = new short[arrayLength];
                for (int i = 0; i < arrayLength; i++) {
                    array[i] = buf.readShort();
                }
                return array;

            } else {
                return null;
            }
        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    public int[] readIntArray() {
        try {
            int arrayLength = buf.readInt();

            if (arrayLength >= 0) {
                int[] array = new int[arrayLength];
                for (int i = 0; i < arrayLength; i++) {
                    array[i] = readInt();
                }
                return array;

            } else {
                return null;
            }
        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
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
        try {
            if (buf.available() > 0) {
                drain();
                throw new IllegalStateException("corrupted message");
            }
        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

    private void drain() {
        try {
            int bytesToSkip = buf.available();
            while (bytesToSkip > 0) {
                bytesToSkip -= buf.skip(bytesToSkip);
            }

        } catch (IOException ex) {
            throw new IllegalStateException("corrupted message");
        }
    }

}
