package com.wepay.waltz.server;

import com.wepay.riff.network.Message;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.common.message.AbstractMessage;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.MessageType;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.server.internal.Partition;
import com.wepay.waltz.test.mock.MockClusterManager;
import com.wepay.waltz.test.mock.MockContext;
import com.wepay.waltz.test.mock.MockStore;
import com.wepay.waltz.test.mock.MockStorePartition;
import com.wepay.waltz.test.mock.MockWaltzClientCallbacks;
import com.wepay.waltz.test.util.PartitionWithMessageBuffering;
import com.wepay.waltz.test.util.StringSerializer;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WaltzServerTest extends WaltzTestBase {

    private static final int BEGINNING = 0;
    private static final int MOUNT_REQUEST_RECEIVED = 0x01;
    private static final int FEED_REQUEST_RECEIVED = 0x02;
    private static final int TRANSACTION_1_RECEIVED = 0x04;
    private static final int TRANSACTION_2_RECEIVED = 0x08;

    private static class PartitionStateChecker {
        private final HashMap<Integer, Partition> allPartitions = new HashMap<>();

        public void check(Map<Integer, Partition> active) {
            // Accumulate all partitions
            allPartitions.putAll(active);

            // Check if only active partitions are open
            allPartitions.forEach((pid, partition) -> {
                if (active.containsKey(pid)) {
                    assertFalse(partition.isClosed());
                } else {
                    assertTrue(partition.isClosed());
                }
            });
        }
    }

    @Test
    public void testPartitionAssignment() throws Exception {
        MockClusterManager clusterManager = new MockClusterManager(3);
        PartitionStateChecker partitionStateChecker = new PartitionStateChecker();

        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(null, clusterManager);
        waltzServerRunner.startAsync();
        WaltzServer server = waltzServerRunner.awaitStart();

        assertEquals(1, clusterManager.managedServers().size());

        ManagedServer managedServer = clusterManager.managedServers().iterator().next();

        setPartitions(managedServer, partition1);
        assertEquals(Utils.set(1), server.partitions().keySet());
        partitionStateChecker.check(server.partitions());

        setPartitions(managedServer, partition1, partition3);
        assertEquals(Utils.set(1, 3), server.partitions().keySet());
        partitionStateChecker.check(server.partitions());

        setPartitions(managedServer, partition3);
        assertEquals(Utils.set(3), server.partitions().keySet());
        partitionStateChecker.check(server.partitions());

        setPartitions(managedServer, partition2, partition3);
        assertEquals(Utils.set(2, 3), server.partitions().keySet());
        partitionStateChecker.check(server.partitions());

        waltzServerRunner.stop();

        assertEquals(0, clusterManager.managedServers().size());
    }

    @Test
    public void testClientConnect() throws Exception {
        Random rand = new Random();
        int partitionId = rand.nextInt(NUM_PARTITIONS);

        MockClusterManager clusterManager = new MockClusterManager(NUM_PARTITIONS);
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(null, clusterManager);

        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks()
            .setClientHighWaterMark(0, -1L)
            .setClientHighWaterMark(1, -1L)
            .setClientHighWaterMark(2, -1L)
            .setClientHighWaterMark(3, -1L);

        WaltzTestClient client = new WaltzTestClient(callbacks, clusterManager);

        try {
            waltzServerRunner.startAsync();
            WaltzServer server = waltzServerRunner.awaitStart();

            ManagedServer managedServer = clusterManager.managedServers().iterator().next();
            setPartitions(managedServer);

            assertEquals(Utils.set(0, 1, 2, 3), server.partitions().keySet());

            ManagedClient managedClient = clusterManager.managedClients().iterator().next();
            setEndpoints(managedClient, managedServer, clusterManager);

            PartitionWithMessageBuffering partition = (PartitionWithMessageBuffering) server.partitions().get(partitionId);

            // The execution is blocked until the connection is established.
            MockContext context1 = MockContext.builder().partitionId(partitionId).data("transaction1").retry(false).build();
            client.submit(context1);

            MockContext context2 = MockContext.builder().partitionId(partitionId).data("transaction2").retry(false).build();
            client.submit(context2);

            int history = BEGINNING;
            AbstractMessage msg;

            for (int i = 0; i < 4; i++) {
                msg = (AbstractMessage) partition.nextMessage(TIMEOUT);
                assertNotNull(msg);

                assertEquals(client.clientId(), msg.reqId.clientId());
                assertEquals(99, msg.reqId.generation());
                assertEquals(partitionId, msg.reqId.partitionId());

                switch (msg.type()) {
                    case MessageType.MOUNT_REQUEST:
                        assertTrue(history == BEGINNING);
                        history |= MOUNT_REQUEST_RECEIVED;
                        break;

                    case MessageType.FEED_REQUEST:
                        assertTrue((history & MOUNT_REQUEST_RECEIVED) != 0);
                        history |= FEED_REQUEST_RECEIVED;
                        break;

                    case MessageType.APPEND_REQUEST:
                        assertTrue((history & MOUNT_REQUEST_RECEIVED) != 0);

                        String data = new String(((AppendRequest) msg).data, StandardCharsets.UTF_8);
                        if (data.equals("transaction1")) {
                            assertTrue((history & (TRANSACTION_1_RECEIVED | TRANSACTION_2_RECEIVED)) == 0);
                            history |= TRANSACTION_1_RECEIVED;
                        } else if (data.equals("transaction2")) {
                            assertTrue((history & TRANSACTION_1_RECEIVED) != 0);
                            history |= TRANSACTION_2_RECEIVED;
                        }
                        break;

                    default:
                        break;
                }
            }

        } finally {
            close(client);
            waltzServerRunner.stop();
        }

        assertEquals(0, clusterManager.managedServers().size());
    }

    @Test
    public void testConcurrencyControlSingleWriteLock() throws Exception {
        MockClusterManager clusterManager = new MockClusterManager(1);
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(null, clusterManager);

        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzTestClient client1 = new WaltzTestClient(callbacks1, clusterManager);
        WaltzTestClient client2 = new WaltzTestClient(callbacks2, clusterManager);

        try {
            waltzServerRunner.startAsync();
            WaltzServer server = waltzServerRunner.awaitStart();

            ManagedServer managedServer = clusterManager.managedServers().iterator().next();
            setPartitions(managedServer, partition0);
            assertEquals(Utils.set(0), server.partitions().keySet());

            Iterator<ManagedClient> iter = clusterManager.managedClients().iterator();
            ManagedClient managedClient1 = iter.next();
            setEndpoints(managedClient1, managedServer, clusterManager, new PartitionInfo(0, 99));
            ManagedClient managedClient2 = iter.next();
            setEndpoints(managedClient2, managedServer, clusterManager, new PartitionInfo(0, 99));

            PartitionWithMessageBuffering partition = (PartitionWithMessageBuffering) server.partitions().get(0);

            // Two transaction1 for lock=1, and another transaction for lock=2
            // They are at the same high-water mark. The second transaction should be prevented by the first transaction.
            // The third transaction should be OK because it has a different payment id.
            MockContext paymentContext1 = MockContext.builder().data("transaction1").writeLocks(1).retry(false).build();
            MockContext paymentContext2 = MockContext.builder().data("transaction2").writeLocks(1).retry(false).build();
            MockContext paymentContext3 = MockContext.builder().data("transaction3").writeLocks(2).retry(false).build();

            AppendRequest transaction1 = client1.buildForTest(paymentContext1);
            AppendRequest transaction2 = client2.buildForTest(paymentContext2);
            AppendRequest transaction3 = client2.buildForTest(paymentContext3);

            Future<Boolean> future1 = client1.appendForTest(transaction1, paymentContext1);

            // Wait for the first append request to reach the partition
            Message msg;
            do {
                msg = partition.nextMessage(TIMEOUT);
            } while (msg != null && msg.type() != MessageType.APPEND_REQUEST);

            assertNotNull(msg);

            Future<Boolean> future2 = client2.appendForTest(transaction2, paymentContext2);
            Future<Boolean> future3 = client2.appendForTest(transaction3, paymentContext3);

            // Transaction1 should be success
            assertTrue(future1.get(10, TimeUnit.SECONDS));

            // Transaction2 should be failure
            assertFalse(future2.get(10, TimeUnit.SECONDS));

            // Transaction3 should be success
            assertTrue(future3.get(10, TimeUnit.SECONDS));

        } finally {
            close(client1, client2);
            waltzServerRunner.stop();
        }

        assertEquals(0, clusterManager.managedServers().size());
    }

    @Test
    public void testConcurrencyControlSingleReadLock() throws Exception {
        MockClusterManager clusterManager = new MockClusterManager(1);
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(null, clusterManager);

        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzTestClient client1 = new WaltzTestClient(callbacks1, clusterManager);
        WaltzTestClient client2 = new WaltzTestClient(callbacks2, clusterManager);

        try {
            waltzServerRunner.startAsync();
            WaltzServer server = waltzServerRunner.awaitStart();

            ManagedServer managedServer = clusterManager.managedServers().iterator().next();
            setPartitions(managedServer, partition0);
            assertEquals(Utils.set(0), server.partitions().keySet());

            Iterator<ManagedClient> iter = clusterManager.managedClients().iterator();
            ManagedClient managedClient1 = iter.next();
            setEndpoints(managedClient1, managedServer, clusterManager, new PartitionInfo(0, 99));
            ManagedClient managedClient2 = iter.next();
            setEndpoints(managedClient2, managedServer, clusterManager, new PartitionInfo(0, 99));

            PartitionWithMessageBuffering partition = (PartitionWithMessageBuffering) server.partitions().get(0);

            // Two transaction1 for lock=1.
            // They are at the same high-water mark. The read lock should allow both transactions.
            MockContext paymentContext1 = MockContext.builder().data("transaction1").readLocks(1).retry(false).build();
            MockContext paymentContext2 = MockContext.builder().data("transaction2").readLocks(1).retry(false).build();

            AppendRequest transaction1 = client1.buildForTest(paymentContext1);
            AppendRequest transaction2 = client2.buildForTest(paymentContext2);

            Future<Boolean> future1 = client1.appendForTest(transaction1, paymentContext1);

            // Wait for the first append request to reach the partition
            Message msg;
            do {
                msg = partition.nextMessage(TIMEOUT);
            } while (msg != null && msg.type() != MessageType.APPEND_REQUEST);

            assertNotNull(msg);

            Future<Boolean> future2 = client2.appendForTest(transaction2, paymentContext2);

            // Transaction1 should be success
            assertTrue(future1.get(10, TimeUnit.SECONDS));

            // Transaction2 should be success
            assertTrue(future2.get(10, TimeUnit.SECONDS));

        } finally {
            close(client1, client2);
            waltzServerRunner.stop();
        }

        assertEquals(0, clusterManager.managedServers().size());
    }

    @Test
    public void testConcurrencyControlMultipleLocks() throws Exception {
        MockClusterManager clusterManager = new MockClusterManager(1);
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(null, clusterManager);

        MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        WaltzTestClient client1 = new WaltzTestClient(callbacks1, clusterManager);
        WaltzTestClient client2 = new WaltzTestClient(callbacks2, clusterManager);

        try {
            waltzServerRunner.startAsync();
            WaltzServer server = waltzServerRunner.awaitStart();

            ManagedServer managedServer = clusterManager.managedServers().iterator().next();
            setPartitions(managedServer, partition0);
            assertEquals(Utils.set(0), server.partitions().keySet());

            Iterator<ManagedClient> iter = clusterManager.managedClients().iterator();
            ManagedClient managedClient1 = iter.next();
            setEndpoints(managedClient1, managedServer, clusterManager, new PartitionInfo(0, 99));
            ManagedClient managedClient2 = iter.next();
            setEndpoints(managedClient2, managedServer, clusterManager, new PartitionInfo(0, 99));

            PartitionWithMessageBuffering partition = (PartitionWithMessageBuffering) server.partitions().get(0);

            // Four transactions for lock=(1,2), lock=(2,3), lock=(3,4), lock(4,5)
            // They are at the same high-water mark. The second and third transactions should be prevented by the first transaction.
            // The fourth transaction should be OK because it has a different lock ids.
            MockContext context1 = MockContext.builder().data("transaction1").writeLocks(1, 2).retry(false).build();
            MockContext context2 = MockContext.builder().data("transaction2").writeLocks(1, 2).retry(false).build();
            MockContext context3 = MockContext.builder().data("transaction3").writeLocks(2, 3).retry(false).build();
            MockContext context4 = MockContext.builder().data("transaction4").writeLocks(3, 4).retry(false).build();

            AppendRequest transaction1 = client1.buildForTest(context1);
            AppendRequest transaction2 = client2.buildForTest(context2);
            AppendRequest transaction3 = client2.buildForTest(context3);
            AppendRequest transaction4 = client2.buildForTest(context4);

            Future<Boolean> future1 = client1.appendForTest(transaction1, context1);

            // Wait for the first append request to reach the partition
            Message msg;
            do {
                msg = partition.nextMessage(TIMEOUT);
            } while (msg != null && msg.type() != MessageType.APPEND_REQUEST);

            assertNotNull(msg);

            Future<Boolean> future2 = client2.appendForTest(transaction2, context2);
            Future<Boolean> future3 = client2.appendForTest(transaction3, context3);
            Future<Boolean> future4 = client2.appendForTest(transaction4, context4);

            // Transaction1 should be success
            assertTrue(future1.get(10, TimeUnit.SECONDS));

            // Transaction2 should be failed
            assertFalse(future2.get(10, TimeUnit.SECONDS));

            // Transaction3 should be failed
            assertFalse(future3.get(10, TimeUnit.SECONDS));

            // Transaction4 should be success
            assertTrue(future4.get(10, TimeUnit.SECONDS));

        } finally {
            close(client1, client2);
            waltzServerRunner.stop();
        }

        assertEquals(0, clusterManager.managedServers().size());
    }

    @Test
    public void testCatchUp() throws Exception {
        final int numTransactionsToSend = 1500;
        MockClusterManager clusterManager = new MockClusterManager(1);
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(null, clusterManager);

        final MockWaltzClientCallbacks callbacks1 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        final MockWaltzClientCallbacks callbacks2 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        final MockWaltzClientCallbacks callbacks3 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        final MockWaltzClientCallbacks callbacks4 = new MockWaltzClientCallbacks().setClientHighWaterMark(0, -1L);
        final WaltzTestClient client1 = new WaltzTestClient(callbacks1, clusterManager);
        final WaltzTestClient client2 = new WaltzTestClient(callbacks2, clusterManager);
        final WaltzTestClient client3 = new WaltzTestClient(callbacks3, clusterManager);
        final WaltzTestClient client4 = new WaltzTestClient(callbacks4, clusterManager);

        try {
            waltzServerRunner.startAsync();
            WaltzServer server = waltzServerRunner.awaitStart();

            ManagedServer managedServer = clusterManager.managedServers().iterator().next();
            setPartitions(managedServer, partition0);
            assertEquals(Utils.set(0), server.partitions().keySet());

            Iterator<ManagedClient> iter = clusterManager.managedClients().iterator();
            ManagedClient managedClient1 = iter.next();
            ManagedClient managedClient2 = iter.next();
            ManagedClient managedClient3 = iter.next();
            ManagedClient managedClient4 = iter.next();

            setEndpoints(managedClient1, managedServer, clusterManager, new PartitionInfo(0, 99));

            // Append many transactions using client1.
            List<Future<Boolean>> futures = new ArrayList<>(numTransactionsToSend);
            for (int i = 0; i < numTransactionsToSend; i++) {
                MockContext context = MockContext.builder().data("transaction" + i).build();
                client1.submit(context);
                futures.add(context.future);
            }

            // Ensure all transactions are done
            long highWaterMark = -1L;
            for (Future<Boolean> future : futures) {
                boolean committed = future.get(10, TimeUnit.SECONDS);
                if (committed) {
                    highWaterMark++;
                }
            }

            callbacks1.awaitHighWaterMark(0, highWaterMark, TIMEOUT);

            // Set high-water marks
            final long hwm3 = highWaterMark - 50;
            final long hwm4 = highWaterMark - 30;
            callbacks2.setClientHighWaterMark(0, -1L); // from scratch
            callbacks3.setClientHighWaterMark(0, hwm3);
            callbacks4.setClientHighWaterMark(0, hwm4);

            // Now connect client2, client3 and client4
            setEndpoints(managedClient2, managedServer, clusterManager, new PartitionInfo(0, 99));
            setEndpoints(managedClient3, managedServer, clusterManager, new PartitionInfo(0, 99));
            setEndpoints(managedClient4, managedServer, clusterManager, new PartitionInfo(0, 99));

            // Make sure clients are connected
            AppendRequest transaction;
            transaction = client2.buildForTest(MockContext.builder().data("transaction" + (numTransactionsToSend + 1)).build());
            assertEquals(highWaterMark, transaction.clientHighWaterMark);

            transaction = client3.buildForTest(MockContext.builder().data("transaction" + (numTransactionsToSend + 2)).build());
            assertEquals(highWaterMark, transaction.clientHighWaterMark);

            transaction = client4.buildForTest(MockContext.builder().data("transaction" + (numTransactionsToSend + 3)).build());
            assertEquals(highWaterMark, transaction.clientHighWaterMark);

            // Verify
            assertEquals(highWaterMark, callbacks2.getClientHighWaterMark(0));
            assertEquals(highWaterMark + 1, callbacks2.reqIds.size());

            assertEquals(highWaterMark, callbacks3.getClientHighWaterMark(0));
            assertEquals(highWaterMark - hwm3, callbacks3.reqIds.size());

            assertEquals(highWaterMark, callbacks4.getClientHighWaterMark(0));
            assertEquals(highWaterMark - hwm4, callbacks4.reqIds.size());

        } catch (Throwable ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            close(client1, client2, client3, client4);
            waltzServerRunner.stop();
        }

        assertEquals(0, clusterManager.managedServers().size());
    }

    @Test
    public void testTransactionFeed() throws Exception {
        Random rand = new Random();

        final int numTransactions = 1500;
        final ArrayList<Object> expected = new ArrayList<>(numTransactions);
        final ArrayList<Object> actual = new ArrayList<>(numTransactions);
        final ArrayList<Future<Boolean>> futures = new ArrayList<>(numTransactions);

        MockClusterManager clusterManager = new MockClusterManager(1);
        MockStore store = new MockStore();
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(null, clusterManager, store);

        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks() {
            @Override
            protected void process(Transaction transaction) {
                try {
                    actual.add(
                        Arrays.asList(
                            transaction.getHeader(),
                            transaction.getTransactionData(StringSerializer.INSTANCE)
                        )
                    );
                } catch (Throwable ex) {
                    actual.add(ex);
                }
            }
        };
        callbacks.setClientHighWaterMark(0, -1L);

        WaltzTestClient client = new WaltzTestClient(callbacks, clusterManager);

        try {
            waltzServerRunner.startAsync();
            WaltzServer server = waltzServerRunner.awaitStart();

            ManagedServer managedServer = clusterManager.managedServers().iterator().next();
            setPartitions(managedServer, partition0);
            assertEquals(Utils.set(0), server.partitions().keySet());

            Iterator<ManagedClient> iter = clusterManager.managedClients().iterator();
            ManagedClient managedClient = iter.next();
            setEndpoints(managedClient, managedServer, clusterManager, new PartitionInfo(0, 99));

            AppendRequest transaction = null;
            Future<Boolean> future;
            for (int i = 0; i < numTransactions; i++) {
                int header = rand.nextInt(Integer.MAX_VALUE);
                String data = "transaction" + i;
                expected.add(Arrays.asList(header, data));
                while (true) {
                    TransactionContext context = MockContext.builder().header(header).data(data).build();
                    transaction = client.buildForTest(context);
                    future = client.appendForTest(transaction, context);

                    // Wait for transaction to be stored and retry.
                    if (future.isDone() && !future.get()) {
                        final long transactionId = i - 1;
                        MockStorePartition storePartition = (MockStorePartition) store.getPartition(0, 99);
                        Uninterruptibly.run(() -> storePartition.await(transactionId, TIMEOUT));
                    } else {
                        break;
                    }
                }

                futures.add(future);
            }

            // Ensure all transactions are applied
            callbacks.awaitReqId(transaction.reqId, TIMEOUT);

            for (Future<Boolean> f : futures) {
                assertTrue(f.isDone());
                assertTrue(f.get());
            }
            assertEquals(expected, actual);

        } finally {
            close(client);
            waltzServerRunner.stop();
        }

        assertEquals(0, clusterManager.managedServers().size());
    }

    public static void close(WaltzClient... clients) {
        for (WaltzClient client : clients) {
            try {
                client.close();
            } catch (Throwable ex) {
                // Ignore
            }
        }
    }

}
