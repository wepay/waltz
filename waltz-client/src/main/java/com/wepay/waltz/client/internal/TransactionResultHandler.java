package com.wepay.waltz.client.internal;

import java.util.function.BiConsumer;

public class TransactionResultHandler implements BiConsumer<Boolean, Throwable> {

    @Override
    public void accept(Boolean success, Throwable exception) {
        if (success != null) {
            if (success) {
                onSuccess();
            } else {
                onFailure();
            }
        } else {
            onException(exception);
        }
    }

    protected void onSuccess() {
    }

    protected void onFailure() {
    }

    protected void onException(Throwable exception) {
    }

}
