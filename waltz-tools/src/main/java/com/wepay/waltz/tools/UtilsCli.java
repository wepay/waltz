package com.wepay.waltz.tools;

import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;

public final class UtilsCli {

    /**
     * A transaction callback to help construct {@link WaltzClient}. It is dummy because
     * it is not suppose to receive any callbacks.
     */
    public static final class DummyTxnCallbacks implements WaltzClientCallbacks {

        @Override
        public long getClientHighWaterMark(int partitionId) {
            return -1L;
        }

        @Override
        public void applyTransaction(Transaction transaction) {
        }

        @Override
        public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
        }
    }
}
