package com.wepay.riff.network;

import java.util.Set;

public class Hello extends Message {

    public static final byte MESSAGE_TYPE = -1;

    public final Set<Short> versions;
    public final String message;

    public Hello(Set<Short> versions, String message) {
        this.versions = versions;
        this.message = message;
    }

    @Override
    public byte type() {
        return MESSAGE_TYPE;
    }

}
