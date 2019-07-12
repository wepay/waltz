package com.wepay.waltz.common.message;

public final class MessageType {

    private MessageType() {
    }

    // Negative message type ids are reserved
    public static final int MOUNT_REQUEST = 0;
    public static final int MOUNT_RESPONSE = 1;
    public static final int APPEND_REQUEST = 2;
    public static final int FEED_REQUEST = 3;
    public static final int FEED_DATA = 4;
    public static final int FEED_SUSPENDED = 5;
    public static final int TRANSACTION_DATA_REQUEST = 6;
    public static final int TRANSACTION_DATA_RESPONSE = 7;
    public static final int FLUSH_REQUEST = 8;
    public static final int FLUSH_RESPONSE = 9;
    public static final int LOCK_FAILURE = 10;

}
