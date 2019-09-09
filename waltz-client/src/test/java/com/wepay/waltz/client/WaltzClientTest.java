package com.wepay.waltz.client;

import com.wepay.waltz.client.internal.mock.MockDriver;
import com.wepay.waltz.client.internal.mock.MockServerPartition;
import com.wepay.waltz.exception.InvalidOperationException;
import com.wepay.waltz.exception.PartitionInactiveException;
import com.wepay.waltz.test.mock.MockContext;
import com.wepay.waltz.test.mock.MockWaltzClientCallbacks;
import com.wepay.waltz.test.util.StringSerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WaltzClientTest {

    @Test
    public void testConcurrencyControlSingleWriteLock() throws Exception {
        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(1);

        MockDriver mockDriver1 = new MockDriver(1, serverPartitions);
        WaltzClientConfig config1 = new WaltzClientConfig(new Properties());
        config1.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver1);
        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client1 = new WaltzClient(callbacks1, config1);

        MockDriver mockDriver2 = new MockDriver(2, serverPartitions);
        WaltzClientConfig config2 = new WaltzClientConfig(new Properties());
        config2.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver2);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client2 = new WaltzClient(callbacks2, config2);

        try {
            mockDriver1.suspendFeed();
            mockDriver2.suspendFeed();

            // Two transactions for lock=1, and another transaction for lock=2
            // They are at the same high-water mark. The second transaction should be prevented by the first transaction.
            // The third transaction should be OK because it has a different lock id.
            MockContext context1 = MockContext.builder().data("transaction1").writeLocks(1).retry(false).build();
            MockContext context2 = MockContext.builder().data("transaction2").writeLocks(1).retry(false).build();
            MockContext context3 = MockContext.builder().data("transaction3").writeLocks(2).retry(false).build();

            client1.submit(context1);
            client2.submit(context2);
            client2.submit(context3);

            mockDriver1.resumeFeed();
            mockDriver2.resumeFeed();

            // Transaction1 should be success
            assertTrue(context1.future.get(10, TimeUnit.SECONDS));

            // Transaction2 should be failure
            assertFalse(context2.future.get(10, TimeUnit.SECONDS));

            // Transaction3 should be success
            assertTrue(context3.future.get(10, TimeUnit.SECONDS));

        } finally {
            close(client1, client2);
        }
    }

    @Test
    public void testConcurrencyControlSingleReadLock() throws Exception {
        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(1);

        MockDriver mockDriver1 = new MockDriver(1, serverPartitions);
        WaltzClientConfig config1 = new WaltzClientConfig(new Properties());
        config1.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver1);
        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client1 = new WaltzClient(callbacks1, config1);

        MockDriver mockDriver2 = new MockDriver(2, serverPartitions);
        WaltzClientConfig config2 = new WaltzClientConfig(new Properties());
        config2.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver2);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client2 = new WaltzClient(callbacks2, config2);

        MockContext context1;
        MockContext context2;

        try {
            mockDriver1.suspendFeed();
            mockDriver2.suspendFeed();

            // Read Lock then Read Lock

            // Two transactions for lock=1.
            // They are at the same high-water mark. The read lock should allow both transaction.
            context1 = MockContext.builder().data("transaction1").readLocks(1).retry(false).build();
            context2 = MockContext.builder().data("transaction2").readLocks(1).retry(false).build();

            client1.submit(context1);
            client2.submit(context2);

            mockDriver1.resumeFeed();
            mockDriver2.resumeFeed();

            // Transaction1 should be success
            assertTrue(context1.future.get(10, TimeUnit.SECONDS));

            // Transaction2 should be success
            assertTrue(context2.future.get(10, TimeUnit.SECONDS));


            // Write Lock then Read Lock

            mockDriver1.suspendFeed();
            mockDriver2.suspendFeed();

            // Two transactions for lock=1
            // They are at the same high-water mark. The second transaction should be prevented.
            context1 = MockContext.builder().data("transaction1").writeLocks(1).retry(false).build();
            context2 = MockContext.builder().data("transaction2").readLocks(1).retry(false).build();

            client1.submit(context1);
            client2.submit(context2);

            mockDriver1.resumeFeed();
            mockDriver2.resumeFeed();

            // Transaction1 should be success
            assertTrue(context1.future.get(10, TimeUnit.SECONDS));

            // Transaction2 should be failure
            assertFalse(context2.future.get(10, TimeUnit.SECONDS));

            // Read Lock then Write Lock

            mockDriver1.suspendFeed();
            mockDriver2.suspendFeed();

            // Two transactions for lock=1
            // They are at the same high-water mark. The second transaction should be prevented.
            context1 = MockContext.builder().data("transaction1").readLocks(1).retry(false).build();
            context2 = MockContext.builder().data("transaction2").writeLocks(1).retry(false).build();

            client1.submit(context1);
            client2.submit(context2);

            mockDriver1.resumeFeed();
            mockDriver2.resumeFeed();

            // Transaction1 should be success
            assertTrue(context1.future.get(10, TimeUnit.SECONDS));

            // Transaction2 should be success
            assertTrue(context2.future.get(10, TimeUnit.SECONDS));

        } finally {
            close(client1, client2);
        }
    }

    @Test
    public void testConcurrencyControlMultipleLocks() throws Exception {
        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(1);

        MockDriver mockDriver1 = new MockDriver(1, serverPartitions);
        WaltzClientConfig config1 = new WaltzClientConfig(new Properties());
        config1.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver1);
        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client1 = new WaltzClient(callbacks1, config1);

        MockDriver mockDriver2 = new MockDriver(2, serverPartitions);
        WaltzClientConfig config2 = new WaltzClientConfig(new Properties());
        config2.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver2);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client2 = new WaltzClient(callbacks2, config2);

        try {
            mockDriver1.suspendFeed();
            mockDriver2.suspendFeed();

            // Four transactions for lock=(1,2), lock=(1, 2), lock=(2, 3), lock(4,5)
            // They are at the same high-water mark. The second and third transactions should be prevented by the first transaction.
            // The fourth transaction should be OK because it has a different lock ids.
            MockContext context1 = MockContext.builder().data("transaction1").writeLocks(1, 2).retry(false).build();
            MockContext context2 = MockContext.builder().data("transaction2").writeLocks(1, 2).retry(false).build();
            MockContext context3 = MockContext.builder().data("transaction3").writeLocks(2, 3).retry(false).build();
            MockContext context4 = MockContext.builder().data("transaction4").writeLocks(4, 5).retry(false).build();

            client1.submit(context1);
            client2.submit(context2);
            client2.submit(context3);
            client2.submit(context4);

            mockDriver1.resumeFeed();
            mockDriver2.resumeFeed();

            // Transaction1 should be success
            assertTrue(context1.future.get(10, TimeUnit.SECONDS));

            // Transaction2 should be failure
            assertFalse(context2.future.get(10, TimeUnit.SECONDS));

            // Transaction3 should be failure
            assertFalse(context3.future.get(10, TimeUnit.SECONDS));

            // Transaction4 should be success
            assertTrue(context4.future.get(10, TimeUnit.SECONDS));

        } finally {
            close(client1, client2);
        }
    }

    @Test
    public void testRetry() throws Exception {
        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(1);

        MockDriver mockDriver1 = new MockDriver(1, serverPartitions);
        WaltzClientConfig config1 = new WaltzClientConfig(new Properties());
        config1.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver1);
        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client1 = new WaltzClient(callbacks1, config1);

        MockDriver mockDriver2 = new MockDriver(2, serverPartitions);
        WaltzClientConfig config2 = new WaltzClientConfig(new Properties());
        config2.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver2);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client2 = new WaltzClient(callbacks2, config2);

        try {
            mockDriver1.suspendFeed();
            mockDriver2.suspendFeed();

            // Four transactions for lock=10. All transaction will succeed eventually by retry.
            MockContext context1 = MockContext.builder().data("transaction1").writeLocks(10).retry(true).build();
            MockContext context2 = MockContext.builder().data("transaction2").writeLocks(10).retry(true).build();
            MockContext context3 = MockContext.builder().data("transaction3").writeLocks(10).retry(true).build();
            MockContext context4 = MockContext.builder().data("transaction4").writeLocks(10).retry(true).build();

            client1.submit(context1);
            client1.submit(context2);
            client1.submit(context3);
            client1.submit(context4);

            mockDriver1.resumeFeed();
            mockDriver2.resumeFeed();

            // All transactions should be successful after retries
            assertTrue(context1.future.get(10, TimeUnit.SECONDS));
            assertTrue(context2.future.get(10, TimeUnit.SECONDS));
            assertTrue(context3.future.get(10, TimeUnit.SECONDS));
            assertTrue(context4.future.get(10, TimeUnit.SECONDS));

            // There must be retried transactions. Total execCount should be greater than 7 (4 + minimum retry count 3)
            int totalExecCount = context1.execCount.get() + context2.execCount.get() + context3.execCount.get() + context4.execCount.get();
            assertTrue(totalExecCount > 7);

        } finally {
            close(client1, client2);
        }
    }

    @Test
    public void testCatchUp() throws Exception {
        final int numTransactionsToSend = 10000;

        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(1);

        MockDriver mockDriver1 = new MockDriver(1, serverPartitions);
        WaltzClientConfig config1 = new WaltzClientConfig(new Properties());
        config1.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver1);
        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);

        MockDriver mockDriver2 = new MockDriver(2, serverPartitions);
        WaltzClientConfig config2 = new WaltzClientConfig(new Properties());
        config2.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver2);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);

        MockDriver mockDriver3 = new MockDriver(3, serverPartitions);
        WaltzClientConfig config3 = new WaltzClientConfig(new Properties());
        config3.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver3);
        MockWaltzClientCallbacks callbacks3 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);

        MockDriver mockDriver4 = new MockDriver(4, serverPartitions);
        WaltzClientConfig config4 = new WaltzClientConfig(new Properties());
        config4.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver4);
        MockWaltzClientCallbacks callbacks4 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);

        WaltzClient client1 = null;
        WaltzClient client2 = null;
        WaltzClient client3 = null;
        WaltzClient client4 = null;
        try {
            client1 = new WaltzClient(callbacks1, config1);

            // Append many transactions using client1.
            List<Future<Boolean>> futures = new ArrayList<>(numTransactionsToSend);
            for (int i = 0; i < numTransactionsToSend; i++) {
                MockContext context = MockContext.builder().data("transaction" + i).retry(true).build();
                client1.submit(context);
                futures.add(context.future);
            }

            // Ensure all transactions are done
            long highWaterMark = -1L;
            for (Future<Boolean> future : futures) {
                future.get(10, TimeUnit.SECONDS);
                highWaterMark++;
            }

            callbacks1.awaitHighWaterMark(0, highWaterMark, 1000 * 10);

            // Set high-water marks
            final long hwm3 = highWaterMark - 50;
            final long hwm4 = highWaterMark - 30;
            callbacks2.setClientHighWaterMark(0, -1L); // from scratch
            callbacks3.setClientHighWaterMark(0, hwm3);
            callbacks4.setClientHighWaterMark(0, hwm4);

            // Now create and start client2, client3 and client4
            client2 = new WaltzClient(callbacks2, config2);
            client3 = new WaltzClient(callbacks3, config3);
            client4 = new WaltzClient(callbacks4, config4);

            callbacks2.awaitHighWaterMark(0, highWaterMark, 1000 * 10);
            callbacks3.awaitHighWaterMark(0, highWaterMark, 1000 * 10);
            callbacks4.awaitHighWaterMark(0, highWaterMark, 1000 * 10);

            // Verify
            assertEquals(highWaterMark + 1, (long) callbacks2.reqIds.size());
            assertEquals(50, (long) callbacks3.reqIds.size());
            assertEquals(30, (long) callbacks4.reqIds.size());

            // Append transactions through client2,3,4
            MockContext context;

            context = MockContext.builder().data("transaction" + (numTransactionsToSend + 1)).retry(true).build();
            client2.submit(context);
            assertTrue(context.future.get(10, TimeUnit.SECONDS));

            context = MockContext.builder().data("transaction" + (numTransactionsToSend + 2)).retry(true).build();
            client3.submit(context);
            assertTrue(context.future.get(10, TimeUnit.SECONDS));

            context = MockContext.builder().data("transaction" + (numTransactionsToSend + 3)).retry(true).build();
            client4.submit(context);
            assertTrue(context.future.get(10, TimeUnit.SECONDS));

            highWaterMark += 3;
            callbacks2.awaitHighWaterMark(0, highWaterMark, 1000 * 10);
            callbacks3.awaitHighWaterMark(0, highWaterMark, 1000 * 10);
            callbacks4.awaitHighWaterMark(0, highWaterMark, 1000 * 10);

            // Verify
            assertEquals(highWaterMark + 1, callbacks2.reqIds.size());
            assertEquals(highWaterMark - hwm3, callbacks3.reqIds.size());
            assertEquals(highWaterMark - hwm4, callbacks4.reqIds.size());

        } catch (Throwable ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            close(client1, client2, client3, client4);
        }
    }

    @Test
    public void testGetTransactionData() throws Exception {
        Random rand = new Random();

        final int numTransactions = 1500;
        final ArrayList<Object> expected = new ArrayList<>(numTransactions);
        final ArrayList<Object> actual = new ArrayList<>(numTransactions);
        final ArrayList<Future<Boolean>> futures = new ArrayList<>(numTransactions);

        MockDriver mockDriver = new MockDriver();
        WaltzClientConfig config = new WaltzClientConfig(new Properties());
        config.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver);

        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks() {
            @Override
            protected void process(Transaction transaction) {
                try {
                    actual.add(transaction.getTransactionData(StringSerializer.INSTANCE));
                } catch (Throwable ex) {
                    actual.add(ex);
                }
            }
        };
        callbacks.setClientHighWaterMark(0, -1L);

        WaltzClient client = new WaltzClient(callbacks, config);
        try {
            for (int i = 0; i < numTransactions; i++) {
                String data = "data=" + rand.nextInt(Integer.MAX_VALUE);
                expected.add(data);

                MockContext context = MockContext.builder().data(data).retry(true).build();
                client.submit(context);

                futures.add(context.future);
            }

            // Ensure all transactions are applied
            for (Future<Boolean> f : futures) {
                f.get(1000, TimeUnit.MILLISECONDS);
            }
            callbacks.awaitHighWaterMark(0, numTransactions, 100);

            for (Future<Boolean> f : futures) {
                assertTrue(f.isDone());
                assertTrue(f.get());
            }
            assertEquals(expected, actual);

        } finally {
            close(client);
        }
    }

    @Test
    public void testAppendFailureDetection() throws Exception {
        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(1);

        MockDriver mockDriver = new MockDriver(1, serverPartitions);
        Map<Object, Object> configParams = Collections.singletonMap(WaltzClientConfig.LONG_WAIT_THRESHOLD, 200);
        WaltzClientConfig config = new WaltzClientConfig(configParams);
        config.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver);
        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzClient client = new WaltzClient(callbacks1, config);

        try {
            MockContext context = MockContext.builder().data("transaction1").retry(false).build();

            mockDriver.forceNextAppendFail();
            client.submit(context);

            // Transaction1 should be failure
            assertFalse(context.future.get(10, TimeUnit.SECONDS));

        } finally {
            close(client);
        }
    }

    @Test
    public void testSetPartitionsAutoMountOn() throws Exception {
        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(3);
        MockDriver mockDriver = new MockDriver(1, serverPartitions);
        Map<Object, Object> configParams = Collections.emptyMap();
        WaltzClientConfig config = new WaltzClientConfig(configParams);
        config.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver);
        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks()
            .setClientHighWaterMark(0, -1L)
            .setClientHighWaterMark(1, -1L)
            .setClientHighWaterMark(2, -1L);
        WaltzClient client = new WaltzClient(callbacks, config);

        try {
            MockContext context = MockContext.builder().data("transaction1").retry(false).build();

            client.submit(context);

            assertTrue(context.future.get(100, TimeUnit.SECONDS));

            try {
                client.setPartitions(Collections.singleton(0));
                fail();

            } catch (Throwable ex) {
                assertTrue(ex instanceof InvalidOperationException);
            }

            assertEquals(serverPartitions.keySet(), client.getPartitions());

        } finally {
            close(client);
        }
    }

    @Test
    public void testSetPartitionsAutoMountOff() throws Exception {
        final ArrayList<Object> expected = new ArrayList<>();
        final ArrayList<Object> actual = new ArrayList<>();

        Map<Integer, MockServerPartition> serverPartitions = MockServerPartition.create(3);
        MockDriver mockDriver = new MockDriver(2, serverPartitions);
        Map<Object, Object> configParams = Collections.singletonMap(WaltzClientConfig.AUTO_MOUNT, false);
        WaltzClientConfig config = new WaltzClientConfig(configParams);
        config.setObject(WaltzClientConfig.MOCK_DRIVER, mockDriver);
        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks() {
            @Override
            protected void process(Transaction transaction) {
                try {
                    actual.add(transaction.getTransactionData(StringSerializer.INSTANCE));
                } catch (Throwable ex) {
                    actual.add(ex);
                }
            }
        };
        callbacks.setClientHighWaterMark(0, -1L).setClientHighWaterMark(1, -1L).setClientHighWaterMark(2, -1L);

        WaltzClient client = new WaltzClient(callbacks, config);

        try {
            assertEquals(Collections.emptySet(), client.getPartitions());

            MockContext context = MockContext.builder().data("transaction1").retry(false).build();
            expected.add("transaction1");

            try {
                client.submit(context);
                fail();

            } catch (Throwable ex) {
                assertTrue(ex instanceof PartitionInactiveException);
            }

            try {
                client.setPartitions(Collections.singleton(0));

            } catch (WaltzClientRuntimeException ex) {
                fail();
            }

            try {
                client.submit(context);

            } catch (WaltzClientRuntimeException ex) {
                fail();
            }

            assertTrue(context.future.get(100, TimeUnit.SECONDS));

            callbacks.awaitHighWaterMark(0, 0, 100);
            assertEquals(expected, actual);

        } finally {
            client.close();
        }

    }

    public static void close(WaltzClient... clients) {
        for (WaltzClient client : clients) {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (Throwable ex) {
                // Ignore
            }
        }
    }

}
