package com.wepay.waltz.client.internal.mock;

import com.wepay.waltz.client.PartitionLocalLock;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.TransactionBuilder;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.client.internal.RpcClient;
import com.wepay.waltz.client.internal.StreamClient;
import com.wepay.waltz.client.internal.TransactionBuilderImpl;
import com.wepay.waltz.client.internal.TransactionFuture;
import com.wepay.waltz.client.internal.TransactionMonitor;
import com.wepay.waltz.client.internal.TransactionResultHandler;
import com.wepay.waltz.common.message.AppendRequest;
import com.wepay.waltz.common.message.ReqId;
import com.wepay.waltz.test.mock.MockWaltzClientCallbacks;
import com.wepay.waltz.test.util.StringSerializer;
import com.wepay.zktools.util.Uninterruptibly;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MockDriverTest {

    private final WaltzClientConfig config = new WaltzClientConfig(new Properties());
    private final Random rand = new Random();
    private final TransactionResultHandler handler = new TransactionResultHandler();

    @Test
    public void testBasic1() throws Exception {
        final int numTransactions = 100;
        MockDriver driver = new MockDriver();

        DummyCallbacks callbacks = new DummyCallbacks();
        callbacks.setClientHighWaterMark(0, -1L);

        driver.initialize(callbacks, config);

        RpcClient rpcClient = driver.getRpcClient();
        StreamClient streamClient = driver.getStreamClient();

        HashMap<Integer, List<Object>> transactions = new HashMap<>();
        HashSet<ReqId> reqIds = new HashSet<>();
        ArrayList<TransactionFuture> futures = new ArrayList<>();

        for (int i = 0; i < numTransactions; i++) {
            int header = rand.nextInt();
            String data = "transaction" + i;
            DummyContext context = new DummyContext(header, data);
            transactions.put(i, Arrays.asList(header, data));

            TransactionBuilderImpl builder = streamClient.getTransactionBuilder(context);
            context.execute(builder);

            AppendRequest transaction = builder.buildRequest();
            reqIds.add(transaction.reqId);

            futures.add(streamClient.append(transaction));

            callbacks.awaitHighWaterMark(0, (long) i, 1000);
        }

        for (TransactionFuture future: futures) {
            assertEquals(true, future.get(1000, TimeUnit.MILLISECONDS));
        }

        assertEquals((long) numTransactions - 1L, callbacks.getClientHighWaterMark(0));
        assertEquals(new HashSet<>(transactions.values()), callbacks.transactions());
        assertEquals(reqIds, callbacks.reqIds);

        for (int i = 0; i < numTransactions; i++) {
            Future<byte[]> future = rpcClient.getTransactionData(0, i);
            String data = new String(future.get(), StandardCharsets.UTF_8);
            assertEquals(transactions.get(i).get(1), data);
        }
    }

    @Test
    public void testBasic2() throws Exception {
        final int numTransactions = 100;

        Map<Integer, MockServerPartition> serverPartitions = Collections.singletonMap(0, new MockServerPartition(0));

        MockDriver driver1 = new MockDriver(1, serverPartitions);
        DummyCallbacks callbacks1 = new DummyCallbacks();
        callbacks1.setClientHighWaterMark(0, -1L);
        driver1.initialize(callbacks1, config);

        StreamClient streamClient1 = driver1.getStreamClient();

        HashSet<List<Object>> transactions1 = new HashSet<>();
        HashSet<ReqId> reqIds1 = new HashSet<>();
        HashSet<List<Object>> transactions2 = new HashSet<>();
        HashSet<ReqId> reqIds2 = new HashSet<>();

        for (int i = 0; i < numTransactions; i++) {
            int header = rand.nextInt();
            String data = "transaction" + i;
            DummyContext context = new DummyContext(header, data);
            transactions1.add(Arrays.asList(header, data));
            if (i >= 50) {
                transactions2.add(Arrays.asList(header, data));
            }

            TransactionBuilderImpl builder = streamClient1.getTransactionBuilder(context);
            context.execute(builder);
            AppendRequest transaction = builder.buildRequest();
            reqIds1.add(transaction.reqId);
            if (i >= 50) {
                reqIds2.add(transaction.reqId);
            }

            streamClient1.append(transaction);

            callbacks1.awaitHighWaterMark(0, (long) i, 1000);
        }

        assertEquals(transactions1, callbacks1.transactions());
        assertEquals(reqIds1, callbacks1.reqIds);

        MockDriver driver2 = new MockDriver(2, serverPartitions);
        DummyCallbacks callbacks2 = new DummyCallbacks();
        callbacks2.setClientHighWaterMark(0, 49L);
        driver2.initialize(callbacks2, config);

        StreamClient streamClient2 = driver2.getStreamClient();

        int header = rand.nextInt();
        String data = "extra transaction";
        DummyContext context = new DummyContext(header, data);
        transactions2.add(Arrays.asList(header, data));

        TransactionBuilderImpl builder = streamClient2.getTransactionBuilder(context);
        context.execute(builder);

        AppendRequest transaction = builder.buildRequest();
        reqIds2.add(transaction.reqId);

        streamClient2.append(transaction);
        streamClient2.flushTransactions();

        callbacks2.awaitHighWaterMark(0, (long) numTransactions, 1000);

        assertEquals(transactions2, callbacks2.transactions());
        assertEquals(reqIds2, callbacks2.reqIds);
    }

    @Test
    public void testConcurrencyControl() throws Exception {
        Map<Integer, MockServerPartition> serverPartitions = Collections.singletonMap(0, new MockServerPartition(0));

        MockDriver driver1 = new MockDriver(1, serverPartitions);
        DummyCallbacks callbacks1 = new DummyCallbacks();
        callbacks1.setClientHighWaterMark(0, -1L);
        driver1.initialize(callbacks1, config);

        StreamClient streamClient1 = driver1.getStreamClient();

        MockDriver driver2 = new MockDriver(2, serverPartitions);
        DummyCallbacks callbacks2 = new DummyCallbacks();
        callbacks2.setClientHighWaterMark(0, 49L);
        driver2.initialize(callbacks2, config);

        StreamClient streamClient2 = driver2.getStreamClient();

        DummyContext context1 = new DummyContext(1, "transaction1", 100);
        DummyContext context2 = new DummyContext(1, "transaction2", 100);

        TransactionBuilderImpl builder1 = streamClient1.getTransactionBuilder(context1);
        context1.execute(builder1);
        TransactionBuilderImpl builder2 = streamClient2.getTransactionBuilder(context2);
        context1.execute(builder2);

        AppendRequest transaction1 = builder1.buildRequest();
        AppendRequest transaction2 = builder2.buildRequest();

        TransactionFuture future1 = streamClient1.append(transaction1);
        TransactionFuture future2 = streamClient2.append(transaction2);

        streamClient1.flushTransactions();
        streamClient2.flushTransactions();

        // Only one transaction is successful.
        boolean result1 = future1.get(1000, TimeUnit.MILLISECONDS);
        boolean result2 = future2.get(1000, TimeUnit.MILLISECONDS);

        assertTrue(result1 ^ result2);
    }

    @Test
    public void testSetFaultRate() throws Exception {
        final int numTransactions = 200;
        MockDriver driver = new MockDriver();

        DummyCallbacks callbacks = new DummyCallbacks();
        callbacks.setClientHighWaterMark(0, -1L);

        driver.initialize(callbacks, config);
        driver.setFaultRate(0.5);

        StreamClient streamClient = driver.getStreamClient();

        int numFaults = 0;
        int numSuccess = 0;

        for (int i = 0; i < numTransactions; i++) {
            DummyContext context = new DummyContext(0, "transaction" + i);

            TransactionBuilderImpl builder = streamClient.getTransactionBuilder(context);
            context.execute(builder);

            AppendRequest request = builder.buildRequest();

            TransactionFuture future = streamClient.append(request);
            streamClient.flushTransactions();

            if (!future.get(1000, TimeUnit.MILLISECONDS)) {
                numFaults++;
            } else {
                callbacks.awaitHighWaterMark(0, (long) numSuccess++, 1000);
            }
        }

        assertTrue(numFaults > 0);
    }

    @Test
    public void testForceNextFail() throws Exception {
        final int numTransactions = 100;
        MockDriver driver = new MockDriver();

        DummyCallbacks callbacks = new DummyCallbacks();
        callbacks.setClientHighWaterMark(0, -1L);

        driver.initialize(callbacks, config);

        StreamClient streamClient = driver.getStreamClient();

        int numSuccess = 0;

        for (int i = 0; i < numTransactions; i++) {
            DummyContext context = new DummyContext(0, "transaction" + i);

            TransactionBuilderImpl builder = streamClient.getTransactionBuilder(context);
            context.execute(builder);

            AppendRequest transaction = builder.buildRequest();

            final boolean forceNextFail = (i % 3) == 0;

            if (forceNextFail) {
                driver.forceNextAppendFail();
            }

            TransactionFuture future = streamClient.append(transaction);
            streamClient.flushTransactions();

            if (forceNextFail) {
                assertFalse(future.get(1000, TimeUnit.MILLISECONDS));
            } else {
                assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

                callbacks.awaitHighWaterMark(0, (long) numSuccess++, 1000);
            }
        }
    }

    @Test
    public void testSuspendFeed() throws Exception {
        final int numTransactions = TransactionMonitor.MAX_CONCURRENT_TRANSACTIONS;

        Map<Integer, MockServerPartition> serverPartitions = Collections.singletonMap(0, new MockServerPartition(0));

        MockDriver driver1 = new MockDriver(1, serverPartitions);
        DummyCallbacks callbacks1 = new DummyCallbacks();
        callbacks1.setClientHighWaterMark(0, -1L);
        driver1.initialize(callbacks1, config);

        MockDriver driver2 = new MockDriver(2, serverPartitions);
        DummyCallbacks callbacks2 = new DummyCallbacks();
        callbacks2.setClientHighWaterMark(0, -1L);
        driver2.initialize(callbacks2, config);
        driver2.suspendFeed();

        StreamClient streamClient1 = driver1.getStreamClient();
        StreamClient streamClient2 = driver2.getStreamClient();

        ArrayList<TransactionFuture> futures = new ArrayList<>();

        int numSuccess = 0;

        for (int i = 0; i < numTransactions; i++) {
            DummyContext context = new DummyContext(0, "transaction" + i);

            TransactionBuilderImpl builder = streamClient1.getTransactionBuilder(context);
            context.execute(builder);

            AppendRequest transaction = builder.buildRequest();

            futures.add(streamClient1.append(transaction));
        }

        streamClient1.flushTransactions();

        callbacks1.awaitHighWaterMark(0, (long) numTransactions - 1L, 1000);

        for (TransactionFuture future: futures) {
            if (future.get(1000, TimeUnit.MILLISECONDS)) {
                numSuccess++;
            }
        }


        assertEquals(numTransactions, numSuccess);
        assertEquals((long) numTransactions - 1L, callbacks1.getClientHighWaterMark(0));

        Uninterruptibly.sleep(50);

        assertEquals(-1L, callbacks2.getClientHighWaterMark(0));

        driver2.resumeFeed();

        DummyContext context = new DummyContext(0, "extra transaction");
        TransactionBuilderImpl builder = streamClient2.getTransactionBuilder(context);
        context.execute(builder);

        AppendRequest request = builder.buildRequest();
        streamClient2.append(request);
        streamClient2.flushTransactions();

        callbacks2.awaitHighWaterMark(0, (long) numTransactions, 1000);

        assertEquals((long) numTransactions, callbacks2.getClientHighWaterMark(0));
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

    private static class DummyContext extends TransactionContext {
        private final int header;
        private final String data;
        private final List<PartitionLocalLock> locks = new ArrayList<>();

        DummyContext(int header, String data, int... locks) {
            super();
            this.header = header;
            this.data = data;
            if (locks != null) {
                for (int id : locks) {
                    this.locks.add(new PartitionLocalLock("test", id));
                }
            }
        }

        @Override
        public int partitionId(int numPartitions) {
            return 0;
        }

        @Override
        public boolean execute(TransactionBuilder builder) {
            builder.setHeader(header);
            builder.setTransactionData(data, StringSerializer.INSTANCE);
            builder.setWriteLocks(locks);
            return true;
        }
    }

    private static class DummyCallbacks extends MockWaltzClientCallbacks {

        private final HashSet<List<Object>> transactions = new HashSet<>();

        @Override
        public void process(Transaction transaction) {
            transactions.add(
                Arrays.asList(transaction.getHeader(), transaction.getTransactionData(StringSerializer.INSTANCE))
            );
        }

        Set<List<Object>> transactions() {
            synchronized (transactions) {
                return new HashSet<>(transactions);
            }
        }
    }

}
