package com.wepay.waltz.client.internal;

import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.exception.InvalidOperationException;
import com.wepay.waltz.exception.PartitionInactiveException;
import com.wepay.waltz.test.mock.MockContext;
import com.wepay.waltz.test.mock.MockWaltzClientCallbacks;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InternalStreamClientTest extends InternalClientTestBase {
    private static final long TIMEOUT = 10000;

    @Test
    public void testAutoMountOn() throws Exception {
        final int numTransactions = 10;
        List<String> expected = new ArrayList<>();

        InternalRpcClient internalRpcClient = getInternalRpcClient(WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS);
        InternalStreamClient internalStreamClient = getInternalStreamClient(true, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, internalRpcClient);

        for (int i = 0; i < numTransactions; i++) {
            String data = "transaction" + i;
            expected.add(data);

            MockContext context = MockContext.builder().header(0).data(data).build();
            TransactionBuilderImpl transactionBuilder = internalStreamClient.getTransactionBuilder(context);
            context.execute(transactionBuilder);

            TransactionFuture future = internalStreamClient.append(transactionBuilder.buildRequest(), context);

            assertTrue(future.get());
        }

        for (int i = 0; i < numTransactions; i++) {
            long transactionId = i;
            byte[] data = Uninterruptibly.call(() -> internalRpcClient.getTransactionData(0, transactionId).get());
            assertEquals(expected.get(i), new String(data, UTF_8));
        }

        internalStreamClient.flushTransactions();

        assertEquals(allPartitions, internalStreamClient.getActivePartitions());

        assertFalse(internalStreamClient.hasPendingTransactions());

        try {
            internalStreamClient.setActivePartitions(Collections.emptySet());
            fail();

        } catch (InvalidOperationException ex) {
            assertTrue(ex.toString().contains("failed to set partitions"));
        }
    }

    @Test
    public void testSetActivePartitions() throws Exception {
        InternalRpcClient internalRpcClient = getInternalRpcClient(WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS);
        InternalStreamClient internalStreamClient = getInternalStreamClient(false, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, internalRpcClient);

        assertEquals(Collections.emptySet(), internalStreamClient.getActivePartitions());
        checkInactive(internalStreamClient, 0);
        checkInactive(internalStreamClient, 1);
        checkInactive(internalStreamClient, 2);

        internalStreamClient.setActivePartitions(Utils.set(1));
        assertEquals(Utils.set(1), internalStreamClient.getActivePartitions());
        checkInactive(internalStreamClient, 0);
        checkActive(internalStreamClient, 1);
        checkInactive(internalStreamClient, 2);

        internalStreamClient.setActivePartitions(Utils.set(1, 2));
        assertEquals(Utils.set(1, 2), internalStreamClient.getActivePartitions());
        checkInactive(internalStreamClient, 0);
        checkActive(internalStreamClient, 1);
        checkActive(internalStreamClient, 2);

        internalStreamClient.setActivePartitions(Utils.set(0, 2));
        assertEquals(Utils.set(0, 2), internalStreamClient.getActivePartitions());
        checkActive(internalStreamClient, 0);
        checkInactive(internalStreamClient, 1);
        checkActive(internalStreamClient, 2);

        internalStreamClient.setActivePartitions(Utils.set(0, 1, 2));
        assertEquals(Utils.set(0, 1, 2), internalStreamClient.getActivePartitions());
        checkActive(internalStreamClient, 0);
        checkActive(internalStreamClient, 1);
        checkActive(internalStreamClient, 2);
    }

    private void checkInactive(InternalStreamClient internalStreamClient, int partitionId) {
        try {
            MockContext context = MockContext.builder().partitionId(partitionId).header(0).data("this should fail").build();
            internalStreamClient.getTransactionBuilder(context);
            fail();

        } catch (Throwable ex) {
            assertTrue(ex instanceof PartitionInactiveException);
        }

        try {
            internalStreamClient.flushTransactions();
            fail();

        } catch (Throwable ex) {
            assertTrue(ex instanceof PartitionInactiveException);
        }
    }

    private void checkActive(InternalStreamClient internalStreamClient, int partitionId) throws Exception {
        MockContext context = MockContext.builder().partitionId(partitionId).header(0).data("ok (partition-1)").build();
        TransactionBuilderImpl transactionBuilder = internalStreamClient.getTransactionBuilder(context);
        context.execute(transactionBuilder);

        TransactionFuture future = internalStreamClient.append(transactionBuilder.buildRequest(), context);

        assertTrue(future.get());
    }

    @Test
    public void testWriteToInactivePartition() throws Exception {
        final int numTransactions = 2000;
        Queue<TransactionFuture> futures = new ConcurrentLinkedQueue<>();

        InternalRpcClient internalRpcClient = getInternalRpcClient(WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS);
        InternalStreamClient internalStreamClient = getInternalStreamClient(false, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, internalRpcClient);
        internalStreamClient.setActivePartitions(Utils.set(0));

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            Uninterruptibly.run(latch::await);

            for (int i = 0; i < numTransactions; i++) {
                String data = "transaction" + i;
                MockContext context = MockContext.builder().header(0).data(data).build();
                TransactionFuture future = null;

                while (future == null) {
                    TransactionBuilderImpl transactionBuilder = internalStreamClient.getTransactionBuilder(context);
                    context.execute(transactionBuilder);
                    future = internalStreamClient.append(transactionBuilder.buildRequest(), context);
                }

                futures.add(future);
            }
        });

        thread.start();
        latch.countDown();
        Uninterruptibly.sleep(500);
        internalStreamClient.setActivePartitions(Collections.emptySet());
        thread.join();

        for (TransactionFuture future : futures) {
            try {
                assertTrue(future.get());

            } catch (ExecutionException ex) {
                assertTrue(ex.getCause().toString().contains("partition inactive"));
            }
        }
    }

    @Test
    public void testOnApplicationWithRetry() throws Exception {
        final int numTransactions = 50;
        Queue<CompletableFuture<Boolean>> applicationFutures = new ConcurrentLinkedQueue<>();

        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks() {
            private volatile long transactionId = -1;
            @Override
            protected void process(Transaction transaction) {
                if (transaction.transactionId > transactionId) {
                    transactionId = transaction.transactionId;
                    throw new RuntimeException(
                        "intentionally failing the first call of applyTransaction for the current transaction"
                    );
                } else {
                    super.process(transaction);
                }
            }
        };
        initClientHighWaterMarks(callbacks);

        InternalRpcClient internalRpcClient = getInternalRpcClient(WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS);
        InternalStreamClient internalStreamClient = getInternalStreamClient(
            true, WaltzClientConfig.DEFAULT_MAX_CONCURRENT_TRANSACTIONS, internalRpcClient, callbacks
        );

        Thread thread = new Thread(() -> {
            for (int i = 0; i < numTransactions; i++) {
                String data = "transaction" + i;
                MockContext context = MockContext.builder().header(0).data(data).build();
                TransactionFuture future = null;

                while (future == null) {
                    TransactionBuilderImpl transactionBuilder = internalStreamClient.getTransactionBuilder(context);
                    context.execute(transactionBuilder);
                    future = internalStreamClient.append(transactionBuilder.buildRequest(), context);

                    try {
                        if (future.get()) {
                            break;
                        }
                    } catch (Exception ex) {
                        // Connection loss was induced by transaction application failure,
                        // and it caused a write failure. Retry.
                        future = null;
                    }
                }

                applicationFutures.add(context.applicationFuture);
            }
        });

        thread.start();
        thread.join();

        for (CompletableFuture<Boolean> future : applicationFutures) {
            assertTrue(future.get(TIMEOUT, TimeUnit.MILLISECONDS));
        }
    }

}
