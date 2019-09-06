package com.wepay.waltz.store.exception;

/**
 * This exception is thrown if the {@link com.wepay.waltz.store.internal.StoreSession}
 * trying to access is closed.
 */
public class SessionClosedException extends Exception {

    /**
     * Class constructor.
     */
    public SessionClosedException() {
        super();
    }

}
