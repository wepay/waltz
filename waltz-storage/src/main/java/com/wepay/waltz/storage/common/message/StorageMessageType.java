package com.wepay.waltz.storage.common.message;

public final class StorageMessageType {

    private StorageMessageType() {
    }

    // Negative message type ids are reserved
    public static final int OPEN_REQUEST = 1;
    public static final int LAST_SESSION_INFO_REQUEST = 2;
    public static final int LAST_SESSION_INFO_RESPONSE = 3;
    public static final int SET_LOW_WATER_MARK_REQUEST = 4;
    public static final int TRUNCATE_REQUEST = 5;
    public static final int APPEND_REQUEST = 6;
    public static final int SUCCESS_RESPONSE = 7;
    public static final int FAILURE_RESPONSE = 8;
    public static final int RECORD_HEADER_REQUEST = 9;
    public static final int RECORD_HEADER_RESPONSE = 10;
    public static final int RECORD_REQUEST = 11;
    public static final int RECORD_RESPONSE = 12;
    public static final int MAX_TRANSACTION_ID_REQUEST = 13;
    public static final int MAX_TRANSACTION_ID_RESPONSE = 14;
    public static final int RECORD_HEADER_LIST_REQUEST = 15;
    public static final int RECORD_HEADER_LIST_RESPONSE = 16;
    public static final int RECORD_LIST_REQUEST = 17;
    public static final int RECORD_LIST_RESPONSE = 18;

}
