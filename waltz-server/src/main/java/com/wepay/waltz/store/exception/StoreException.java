package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if the store instance creation
 * is failed.
 */
public class StoreException extends Exception {

    /**
     * Class constructor.
     */
    public StoreException() {
        super();
    }

    /**
     * Class constructor.
     * @param cause The cause of the failure.
     */
    public StoreException(Throwable cause) {
        super(cause);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public StoreException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param msg The exception message.
     * @param cause The cause of the failure.
     */
    public StoreException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
