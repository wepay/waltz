package com.wepay.riff.message;

import com.wepay.riff.network.MessageAttributeWriter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ByteArrayMessageAttributeWriter extends MessageAttributeWriter {

    private final ByteArrayOutputStream baos;
    private final DataOutputStream buf;
    private int bytesWritten;

    public ByteArrayMessageAttributeWriter() {
        this.baos = new ByteArrayOutputStream();
        this.buf = new DataOutputStream(baos);
        bytesWritten = 0;
    }

    public void writeByte(byte b) {
        try {
            buf.write((int) b);
            bytesWritten += 1;
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

    public void writeShort(short s) {
        try {
            buf.writeShort((int) s);
            bytesWritten += 2;
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

    public void writeInt(int v) {
        try {
            buf.writeInt(v);
            bytesWritten += 4;
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

    public void writeLong(long v) {
        try {
            buf.writeLong(v);
            bytesWritten += 8;
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

    public void writeDouble(double v) {
        try {
            buf.writeDouble(v);
            bytesWritten += 8;
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

    public void writeByteArray(byte[] array) {
        try {
            if (array != null) {
                buf.writeInt(array.length);
                buf.write(array);
                bytesWritten += (4 + array.length);

            } else {
                buf.writeInt(-1);
                bytesWritten += 4;
            }
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

    public void writeShortArray(short[] array) {
        try {
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
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

    public void writeIntArray(int[] array) {
        try {
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
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
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

    public byte[] toByteArray() {
        try {
            buf.flush();
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("message corrupted");
        }
    }

}
