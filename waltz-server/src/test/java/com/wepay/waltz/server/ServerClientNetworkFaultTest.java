package com.wepay.waltz.server;

import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.common.message.Record;
import com.wepay.waltz.common.util.Utils;
import com.wepay.waltz.test.mock.MockClusterManager;
import com.wepay.waltz.test.mock.MockContext;
import com.wepay.waltz.test.mock.MockStore;
import com.wepay.waltz.test.mock.MockStorePartition;
import com.wepay.waltz.test.mock.MockWaltzClientCallbacks;
import com.wepay.waltz.test.util.StringSerializer;
import com.wepay.waltz.test.util.WaltzServerRunner;
import com.wepay.waltz.test.util.WaltzTestClientDriver;
import com.wepay.zktools.clustermgr.ManagedClient;
import com.wepay.zktools.clustermgr.ManagedServer;
import com.wepay.zktools.clustermgr.PartitionInfo;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class ServerClientNetworkFaultTest extends WaltzTestBase {

    @Test
    public void test() throws Exception {
        Random rand = new Random();

        final int numTransactions = 1000;
        final List<String> expected = new ArrayList<>(numTransactions);
        final List<String> stored = new ArrayList<>(numTransactions);
        final List<String> applied = new ArrayList<>(numTransactions);

        int proxyPort = portFinder.getPort();
        int realPort = portFinder.getPort();

        MockClusterManager clusterManager = new MockClusterManager(1);
        MockStore store = new MockStore();
        WaltzServerRunner waltzServerRunner = getWaltzServerRunner(realPort, null, clusterManager, store);

        MockWaltzClientCallbacks callbacks = new MockWaltzClientCallbacks() {
            @Override
            protected void process(Transaction transaction) {
                synchronized (applied) {
                    applied.add(transaction.getTransactionData(StringSerializer.INSTANCE));
                }
            }
        };
        callbacks.setClientHighWaterMark(0, -1L);

        WaltzTestClientDriver testDriver = new WaltzTestClientDriver(true, null, callbacks, clusterManager);
        ProxyClientDriver driver = new ProxyClientDriver(testDriver, proxyPort, realPort);
        WaltzClient client = new WaltzClient(driver, 1, 5000);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            waltzServerRunner.startAsync();
            WaltzServer server = waltzServerRunner.awaitStart();

            ManagedServer managedServer = clusterManager.managedServers().iterator().next();
            setPartitions(managedServer, partition0);
            assertEquals(Utils.set(0), server.partitions().keySet());

            Iterator<ManagedClient> iter = clusterManager.managedClients().iterator();
            ManagedClient managedClient = iter.next();
            setEndpoints(managedClient, managedServer, clusterManager, new PartitionInfo(0, 99));

            Runnable faultInjector = () -> {
                int repeats = rand.nextInt(5) + 1;
                for (int n = 0; n < repeats; n++) {
                    driver.disconnect();
                }
            };

            for (int i = 0; i < numTransactions; i++) {
                String data = "transaction" + i;
                expected.add(data);
                client.submit(MockContext.builder().header(0).data(data).build());

                if (i % 100 == 0) {
                    executorService.submit(faultInjector);
                }
            }

            // Ensure all transactions are appended
            client.flushTransactions();
            while (client.hasPendingTransactions()) {
                client.flushTransactions();
            }

            // Get all transaction record from the store
            for (Record record : ((MockStorePartition) store.getPartition(0, 0)).getAllRecords()) {
                stored.add(StringSerializer.INSTANCE.deserialize(record.data));
            }

            // Wait until all transactions are applied
            for (int i = 0; i < 100; i++) {
                if (applied.size() >= stored.size()) {
                    break;
                }
                Uninterruptibly.sleep(50);
            }

            synchronized (applied) {
                // Check if we get all appended transactions
                assertEquals(stored, applied);

                // Check if transactions succeeded
                Collections.sort(expected);
                Collections.sort(stored);
                assertEquals(expected, stored);
            }

        } finally {
            close(client);
            close(driver);
            waltzServerRunner.stop();
            executorService.shutdownNow();
        }

        assertEquals(0, clusterManager.managedServers().size());
    }

    public static void close(WaltzClient client) {
        try {
            client.close();
        } catch (Throwable ex) {
            // Ignore
        }
    }

    public static void close(ProxyClientDriver driver) {
        try {
            driver.close();
        } catch (Throwable ex) {
            // Ignore
        }
    }

}
