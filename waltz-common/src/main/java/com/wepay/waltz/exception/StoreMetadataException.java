package com.wepay.waltz.exception;

/**
 * This exception is thrown if unable to read the metadata
 * from the store.
 */
public class StoreMetadataException extends Exception {

    /**
     * Class constructor.
     */
    public StoreMetadataException() {
        super();
    }

    /**
     * Class constructor.
     * @param cause The cause of the failure.
     */
    public StoreMetadataException(Throwable cause) {
        super(cause);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public StoreMetadataException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     * @param cause The cause of the failure.
     */
    public StoreMetadataException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
