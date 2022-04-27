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
    public static final int HIGH_WATER_MARK_REQUEST = 11;
    public static final int HIGH_WATER_MARK_RESPONSE = 12;
    public static final int CHECK_STORAGE_CONNECTIVITY_REQUEST = 13;
    public static final int CHECK_STORAGE_CONNECTIVITY_RESPONSE = 14;
    public static final int SERVER_PARTITIONS_ASSIGNMENT_REQUEST = 15;
    public static final int SERVER_PARTITIONS_ASSIGNMENT_RESPONSE = 16;
    public static final int ADD_PREFERRED_PARTITION_REQUEST = 17;
    public static final int ADD_PREFERRED_PARTITION_RESPONSE = 18;
    public static final int REMOVE_PREFERRED_PARTITION_REQUEST = 19;
    public static final int REMOVE_PREFERRED_PARTITION_RESPONSE = 20;
    public static final int SERVER_PARTITIONS_HEALTH_STAT_REQUEST = 21;
    public static final int SERVER_PARTITIONS_HEALTH_STAT_RESPONSE = 22;

}
