package com.wepay.riff.network;

public class MockMessage extends Message {

    public static final byte MESSAGE_TYPE = 10;

    public final String message;

    MockMessage(String message) {
        this.message = message;
    }

    @Override
    public byte type() {
        return 10;
    }

}
