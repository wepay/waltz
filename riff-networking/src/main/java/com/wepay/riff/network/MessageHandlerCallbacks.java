package com.wepay.riff.network;

public interface MessageHandlerCallbacks {

    void onChannelActive();

    void onChannelInactive();

    void onWritabilityChanged(boolean isWritable);

    void onExceptionCaught(Throwable ex);

}
