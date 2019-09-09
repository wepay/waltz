package com.wepay.riff.network;

public abstract class MessageAttributeWriter {

    public abstract void writeByte(byte b);

    public abstract void writeShort(short s);

    public abstract void writeInt(int v);

    public abstract void writeLong(long v);

    public abstract void writeDouble(double v);

    public abstract void writeByteArray(byte[] array);

    public abstract void writeShortArray(short[] array);

    public abstract void writeIntArray(int[] array);

    public abstract void writeBoolean(boolean b);

    public abstract void writeString(String text);

    public abstract int bytesWritten();

}
