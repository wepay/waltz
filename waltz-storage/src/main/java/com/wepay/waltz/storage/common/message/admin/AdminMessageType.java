package com.wepay.waltz.storage.common.message.admin;

public final class AdminMessageType {

    private AdminMessageType() {
    }

    // Negative message type ids are reserved
    public static final int SUCCESS_RESPONSE = 1;
    public static final int FAILURE_RESPONSE = 2;
    public static final int OPEN_REQUEST = 3;
    public static final int PARTITION_AVAILABLE_REQUEST = 4;
    public static final int PARTITION_ASSIGNMENT_REQUEST = 6;
    public static final int RECORD_LIST_REQUEST = 7;
    public static final int RECORD_LIST_RESPONSE = 8;
    public static final int LAST_SESSION_INFO_REQUEST = 9;
    public static final int LAST_SESSION_INFO_RESPONSE = 10;
    public static final int METRICS_REQUEST = 11;
    public static final int METRICS_RESPONSE = 12;
    public static final int ASSIGNED_PARTITION_STATUS_REQUEST = 13;
    public static final int ASSIGNED_PARTITION_STATUS_RESPONSE = 14;

}
