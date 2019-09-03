package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if failed to parse the store
 * config file.
 */
public class StoreConfigException extends StoreException {

    /**
     * Class constructor.
     * @param msg The exception message.
     */
    public StoreConfigException(String msg) {
        super(msg);
    }

    /**
     * Class constructor.
     * @param cause The cause of the failure.
     */
    public StoreConfigException(Throwable cause) {
        super(cause);
    }

}
