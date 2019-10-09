package com.wepay.waltz.tools.performance;

import com.wepay.waltz.client.PartitionLocalLock;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.TransactionBuilder;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.exception.SubCommandFailedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The abstract class extends {@link Cli}, which includes some basic command
 * and common functions shared by extending classes.
 */
public final class PerformanceCli extends SubcommandCli {

    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int BYTES_IN_MEGABYTE = 1024 * 1024;
    private static final int DEFAULT_NUMBER_ACTIVE_PARTITIONS = 1;
    private static final Random RANDOM = new Random();

    private PerformanceCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
                new Subcommand(RunProducers.NAME, RunProducers.DESCRIPTION, RunProducers::new),
                new Subcommand(RunConsumers.NAME, RunConsumers.DESCRIPTION, RunConsumers::new)
        ));
    }

    private static final class RunProducers extends PerformanceBase {
        private static final String NAME = "test-producers";
        private static final String DESCRIPTION = "Test and analyze performance of Waltz producer";

        private static final int LAMBDA = 1;
        private static final int EXTRA_TRANSACTION_PER_THREAD = 1;
        private static final int DEFAULT_LOCK_POOL_SIZE = 0;

        private final AtomicLong totalResponseTimeMilli;
        private final Set<Integer> toMountClients;

        private int txnPerThread;
        private int numThread;
        private int avgInterval;
        private int lockPoolSize;
        private int totalRetry;
        private CountDownLatch allClientsReady;
        private CountDownLatch allMountComplete;

        private RunProducers(String[] args) {
            super(args);
            this.totalResponseTimeMilli = new AtomicLong(0);
            this.lockPoolSize = DEFAULT_LOCK_POOL_SIZE;
            this.toMountClients = new HashSet<>();
        }

        @Override
        protected void configureOptions(Options options) {
            Option txnSizeOption = Option.builder("s")
                    .longOpt("txn-size")
                    .desc("Specify size of each transaction")
                    .hasArg()
                    .build();
            Option txnPerThreadOption = Option.builder("p")
                    .longOpt("txn-per-thread")
                    .desc("Specify number of transactions per thread")
                    .hasArg()
                    .build();
            Option numThreadOption = Option.builder("t")
                    .longOpt("num-thread")
                    .desc("Specify number of total threads")
                    .hasArg()
                    .build();
            Option intervalOption = Option.builder("i")
                    .longOpt("interval")
                    .desc("Specify average interval(millisecond) between transactions")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config")
                    .hasArg()
                    .build();
            Option numActivePartitionOption = Option.builder("ap")
                    .longOpt("num-active-partitions")
                    .desc(String.format("Specify number of partitions to interact with. For example, if set to 3, transactions will be"
                            + "evenly distributed among partition 0, 1 and 2. Default to %d", DEFAULT_NUMBER_ACTIVE_PARTITIONS))
                    .hasArg()
                    .build();
            Option lockPoolSizeOption = Option.builder("l")
                    .longOpt("lock-pool-size")
                    .desc(String.format("Specify size of lock pool. Greater the size, less likely transactions get rejected."
                            + "No transaction gets rejected when size is 0. Default to %d", DEFAULT_LOCK_POOL_SIZE))
                    .hasArg()
                    .build();

            txnSizeOption.setRequired(true);
            txnPerThreadOption.setRequired(true);
            numThreadOption.setRequired(true);
            intervalOption.setRequired(true);
            cliCfgOption.setRequired(true);
            numActivePartitionOption.setRequired(false);
            lockPoolSizeOption.setRequired(false);

            options.addOption(txnSizeOption);
            options.addOption(txnPerThreadOption);
            options.addOption(numThreadOption);
            options.addOption(intervalOption);
            options.addOption(cliCfgOption);
            options.addOption(numActivePartitionOption);
            options.addOption(lockPoolSizeOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                // check required arguments
                txnSize = Integer.parseInt(cmd.getOptionValue("txn-size"));
                if (txnSize < 0) {
                    throw new IllegalArgumentException("Found negative: txn-size must be equal or greater than 0");
                }
                data = new byte[txnSize];
                txnPerThread = Integer.parseInt(cmd.getOptionValue("txn-per-thread"));
                if (txnPerThread < 0) {
                    throw new IllegalArgumentException("Found negative: txn-per-thread must be greater or equals to 0");
                }
                numThread = Integer.parseInt(cmd.getOptionValue("num-thread"));
                if (numThread < 0) {
                    throw new IllegalArgumentException("Found negative: num-thread must be greater or equals to 0");
                }
                avgInterval = Integer.parseInt(cmd.getOptionValue("interval"));
                if (avgInterval < 0) {
                    throw new IllegalArgumentException("Found negative: interval must be greater or equals to 0");
                }
                waltzClientConfig = getWaltzClientConfig(cmd.getOptionValue("cli-config-path"));

                // check optional argument
                if (cmd.hasOption("num-active-partitions")) {
                    numActivePartitions = Integer.parseInt(cmd.getOptionValue("num-active-partitions"));
                    if (numActivePartitions < 1) {
                        throw new IllegalArgumentException("Number of active partitions must be greater or equals to 1");
                    }
                }
                if (cmd.hasOption("lock-pool-size")) {
                    lockPoolSize = Integer.parseInt(cmd.getOptionValue("lock-pool-size"));
                    if (lockPoolSize < 0) {
                        throw new IllegalArgumentException("Found negative: lock-pool-size must be greater or equals to 0");
                    }
                }

                int totalTxnSent = (txnPerThread + EXTRA_TRANSACTION_PER_THREAD) * numThread;
                allClientsReady = new CountDownLatch(numThread);
                allMountComplete = new CountDownLatch(numThread);
                allTxnReceived = new CountDownLatch(totalTxnSent);

                ExecutorService executor = Executors.newFixedThreadPool(numThread);
                for (int i = 0; i < numThread; i++) {
                    DummyCallbacks callbacks = new DummyCallbacks();
                    WaltzClient client = new WaltzClient(callbacks, waltzClientConfig);
                    toMountClients.add(client.clientId());

                    // all thread start, but transactions won't be fired until all clients are ready
                    executor.execute(new ProducerThread(txnPerThread, client));
                    allClientsReady.countDown();
                }

                // wait until all transactions complete
                allTxnReceived.await();
                printStatistic();

                executor.shutdown();
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to run producer performance test: %s", e.getMessage()));
            }
        }

        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }

        /**
         * Return next interval between transactions of a client, so that transaction events follow poisson distribution.
         *
         * @return next exponential distributed interval in millisecond.
         */
        private int nextExponentialDistributedInterval() {
            // LAMBDA defaults to 1, so average interval is decided by avgInterval
            return (int) (avgInterval * (Math.log(1 - Math.random()) / -LAMBDA));
        }

        private void printStatistic() {
            long totalElapseMilliSecs = totalResponseTimeMilli.get();
            long totalTxnSent = txnPerThread * numThread;
            long totalBytesSent = totalTxnSent * txnSize;
            double duration = 1.0 * (System.currentTimeMillis() - startTime.get()) / MILLISECONDS_IN_SECOND;

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Appended %d transactions, with total %.4f MB, in %.2f secs%n", totalTxnSent, (1.0 * totalBytesSent / BYTES_IN_MEGABYTE), duration));
            sb.append(String.format("Transaction/sec: %.4f%n", (1.0 * totalTxnSent / duration)));
            sb.append(String.format("MB/sec: %.4f%n", totalBytesSent / duration / BYTES_IN_MEGABYTE));
            sb.append(String.format("Retry/Transaction: %.4f%n", (1.0 * totalRetry / totalTxnSent)));
            sb.append(String.format("MilliSec/Transaction(client side congestion excluded): %.4f", (1.0 * totalElapseMilliSecs / totalTxnSent)));
            System.out.println(sb.toString());
        }

        /**
         * This class extends {@link TransactionContext}, which encapsulates code to build a dummy transaction
         * with specific size of data. Latency will be calculated each {@code onCompletion()} callback.
         */
        protected final class DummyTxnContext extends TransactionContext {

            private static final String LOCK_NAME = "performance_analysis_lock";
            private final byte[] data;
            private final boolean ignoreLatency;
            private Long timestampMilliSec;
            private int execCount;
            private int partitionId;

            DummyTxnContext(byte[] data) {
                this(data, false);
            }

            DummyTxnContext(byte[] data, boolean ignoreLatency) {
                this.data = data;
                this.ignoreLatency = ignoreLatency;
                this.timestampMilliSec = null;
                this.execCount = 0;
                this.partitionId = RANDOM.nextInt(numActivePartitions);
            }

            public int partitionId(int numPartitions) {
                return partitionId;
            }

            @Override
            public boolean execute(TransactionBuilder builder) {
                if (execCount++ > 0) {
                    totalRetry++;
                }
                // Write dummy data
                builder.setTransactionData(data, DummySerializer.INSTANCE);
                if (lockPoolSize != 0) {
                    int lockId = RANDOM.nextInt(lockPoolSize);
                    builder.setWriteLocks(Collections.singletonList(new PartitionLocalLock(LOCK_NAME, lockId)));
                }
                return true;
            }

            @Override
            public void onCompletion(boolean result) {
                if (!ignoreLatency && timestampMilliSec != null) {
                    long latency = System.currentTimeMillis() - timestampMilliSec;
                    totalResponseTimeMilli.addAndGet(latency);
                }
                allTxnReceived.countDown();
            }

            @Override
            public void onException(Throwable ex) {
                System.out.println(ex);
            }
        }

        /**
         * This class implements {@link BaseCallbacks}. Abstract method applyTransaction(Transaction transaction)
         * is implemented to count mounting complete, and update high water mark.
         */
        private class DummyCallbacks extends BaseCallbacks {

            @Override
            public void applyTransaction(Transaction transaction) {
                // when a client receives its own special transaction, allMountComplete count down by 1
                if (toMountClients.size() > 0 && toMountClients.contains(transaction.reqId.clientId())) {
                    toMountClients.remove(transaction.reqId.clientId());
                    allMountComplete.countDown();
                }
                // update client side high water mark
                if (transaction.transactionId - 1 == clientHighWaterMark) {
                    clientHighWaterMark = transaction.transactionId;
                }
            }
        }

        /**
         * This class implements {@link Runnable}, which waits for all clients get ready, iteratively fires all
         * transactions with an exponential distributed interval.
         */
        private final class ProducerThread implements Runnable {

            private int txnPerThread;
            private WaltzClient client;

            ProducerThread(int txnPerThread, WaltzClient client) {
                this.txnPerThread = txnPerThread;
                this.client = client;
            }

            @Override
            public void run() {
                try {
                    allClientsReady.await();

                    // each client send a extra transaction to wait for mounting complete
                    // so we need to exclude this transaction when calculating latency
                    client.submit(new DummyTxnContext(data, true));

                    // start timer when all mounting complete
                    allMountComplete.await();
                    startTime.compareAndSet(0, System.currentTimeMillis());

                    // fire all transactions
                    for (int j = 0; j < txnPerThread; j++) {
                        DummyTxnContext txnContext = new DummyTxnContext(data);
                        client.submit(txnContext);
                        txnContext.timestampMilliSec = System.currentTimeMillis();

                        // By adjusting the distribution parameter of the random sleep,
                        // we can test various congestion scenarios.
                        Thread.sleep(nextExponentialDistributedInterval());
                    }
                } catch (Exception ex) {
                    throw new SubCommandFailedException(ex);
                } finally {
                    try {
                        allTxnReceived.await();
                    } catch (Exception ex) {
                        client.close();
                        throw new SubCommandFailedException(ex);
                    } finally {
                        client.close();
                    }
                }
            }
        }
    }

    private static final class RunConsumers extends PerformanceBase {
        private static final String NAME = "test-consumers";
        private static final String DESCRIPTION = "Test and analyze performance of Waltz consumer";
        private static final int DEFAULT_NUM_PRODUCERS = 100;

        private int numTxn;
        private CountDownLatch allTxnRead;

        private RunConsumers(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option txnSizeOption = Option.builder("s")
                    .longOpt("txn-size")
                    .desc("Specify size of each transaction")
                    .hasArg()
                    .build();
            Option numTxnOption = Option.builder("t")
                    .longOpt("num-txn")
                    .desc("Specify the number of transactions to send")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config")
                    .hasArg()
                    .build();
            Option numActivePartitionOption = Option.builder("ap")
                    .longOpt("num-active-partitions")
                    .desc(String.format("Specify number of active partitions to interact with. For example, if set to 3, transactions will be evenly"
                            + "distributed among partition 0, 1 and 2. Default to %d", DEFAULT_NUMBER_ACTIVE_PARTITIONS))
                    .hasArg()
                    .build();
            txnSizeOption.setRequired(true);
            numTxnOption.setRequired(true);
            cliCfgOption.setRequired(true);
            numActivePartitionOption.setRequired(false);

            options.addOption(txnSizeOption);
            options.addOption(numTxnOption);
            options.addOption(cliCfgOption);
            options.addOption(numActivePartitionOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                // check required argument
                txnSize = Integer.parseInt(cmd.getOptionValue("txn-size"));
                if (txnSize < 0) {
                    throw new IllegalArgumentException("Found negative: txn-size must be equal or greater than 0");
                }
                data = new byte[txnSize];
                numTxn = Integer.parseInt(cmd.getOptionValue("num-txn"));
                if (numTxn < 0) {
                    throw new IllegalArgumentException("Found negative: num-txn must be greater or equal to 0");
                }
                allTxnReceived = new CountDownLatch(numTxn);
                allTxnRead = new CountDownLatch(numTxn);
                waltzClientConfig = getWaltzClientConfig(cmd.getOptionValue("cli-config-path"));

                // check optional argument
                if (cmd.hasOption("num-active-partitions")) {
                    numActivePartitions = Integer.parseInt(cmd.getOptionValue("num-active-partitions"));
                    if (numActivePartitions < 1) {
                        throw new IllegalArgumentException("Number of active partitions must be greater of equals to 1");
                    }
                }

                // produce transactions to consume
                // since we only care about consumer performance, we can create
                // transactions with multiple producer to expedite the test
                int numThread = numTxn > DEFAULT_NUM_PRODUCERS ? DEFAULT_NUM_PRODUCERS : numTxn;
                int txnPerThread = numTxn / numThread;
                ExecutorService executor = Executors.newFixedThreadPool(numThread);
                for (int i = 0; i < numThread; i++) {
                    ProducerCallbacks callbacks = new ProducerCallbacks();
                    WaltzClient client = new WaltzClient(callbacks, waltzClientConfig);
                    executor.execute(new ProducerThread(txnPerThread, client));
                }

                // consume transactions when all committed
                new ConsumerThread(waltzClientConfig).run();

                // wait until all transaction consumed
                allTxnRead.await();

                printStatistic();

                executor.shutdown();
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to run consumer performance test: %s", e.getMessage()));
            }
        }

        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }

        private void printStatistic() {
            long totalBytesRead = numTxn * txnSize;
            double duration = 1.0 * (System.currentTimeMillis() - startTime.get()) / MILLISECONDS_IN_SECOND;

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Read %d transactions, with total %.4f MB, in %.2f secs%n", numTxn, (1.0 * totalBytesRead / BYTES_IN_MEGABYTE), duration));
            sb.append(String.format("Transaction/sec: %.4f%n", (1.0 * numTxn / duration)));
            sb.append(String.format("MB/sec: %.4f", totalBytesRead / duration / BYTES_IN_MEGABYTE));
            System.out.println(sb.toString());
        }

        /**
         * This class extends {@link TransactionContext}, which encapsulates code to build a dummy transaction
         * with specific size of data.
         */
        protected final class DummyTxnContext extends TransactionContext {

            private final byte[] data;

            DummyTxnContext(byte[] data) {
                this.data = data;
            }

            public int partitionId(int numPartitions) {
                return RANDOM.nextInt(numActivePartitions);
            }

            @Override
            public boolean execute(TransactionBuilder builder) {
                // Write dummy data
                builder.setTransactionData(data, DummySerializer.INSTANCE);
                return true;
            }

            @Override
            public void onCompletion(boolean result) {
                allTxnReceived.countDown();
            }

            @Override
            public void onException(Throwable ex) {
                System.out.println(ex);
            }
        }

        /**
         * This class implements {@link Runnable}, which iteratively fires transactions.
         */
        private final class ProducerThread implements Runnable {

            private int txnPerThread;
            private WaltzClient client;

            ProducerThread(int txnPerThread, WaltzClient client) {
                this.txnPerThread = txnPerThread;
                this.client = client;
            }

            @Override
            public void run() {
                try {
                    // fire all transactions
                    for (int j = 0; j < txnPerThread; j++) {
                        DummyTxnContext txnContext = new DummyTxnContext(data);
                        client.submit(txnContext);
                    }
                } catch (Exception ex) {
                    throw new SubCommandFailedException(ex);
                } finally {
                    try {
                        allTxnReceived.await();
                    } catch (Exception ex) {
                        client.close();
                        throw new SubCommandFailedException(ex);
                    } finally {
                        client.close();
                    }
                }
            }
        }

        /**
         * This class implements {@link Runnable}, which measure consumer performance with following steps:
         * 1. Submit N transactions with producer client
         * 2. When all transactions received, consumer starts mounting
         * 3. When first callback received from producer client, timer starts
         * 4. When all N records are read from feeds, timer stops
         * 5. Collect statistic
         */
        private final class ConsumerThread implements Runnable {

            private WaltzClient consumer;
            private WaltzClientConfig config;

            ConsumerThread(WaltzClientConfig config) {
                this.config = config;
            }

            @Override
            public void run() {
                try {
                    // wait until all transactions received before read transaction
                    allTxnReceived.await();

                    // consumer starts to receive feeds until allTxnRead count down to 0
                    consumer = new WaltzClient(new ConsumerCallbacks(), config);
                } catch (NumberFormatException ex) {
                    throw new SubCommandFailedException(ex);
                } catch (Exception ex) {
                    throw new SubCommandFailedException(ex);
                } finally {
                    try {
                        allTxnRead.await();
                    } catch (Exception ex) {
                        consumer.close();
                        throw new SubCommandFailedException(ex);
                    } finally {
                        consumer.close();
                    }
                }
            }
        }

        /**
         * This class implements {@link BaseCallbacks}. Abstract method applyTransaction(Transaction transaction)
         * is implement to start timer and read transaction data.
         */
        private class ConsumerCallbacks extends BaseCallbacks {

            @Override
            public void applyTransaction(Transaction transaction) {
                // start timer when first callback from producer client is received
                startTime.compareAndSet(0, System.currentTimeMillis());

                // read transaction data
                transaction.getTransactionData(DummySerializer.INSTANCE);
                allTxnRead.countDown();
            }
        }

        /**
         * This class implements {@link BaseCallbacks}, Abstract method applyTransaction(Transaction transaction)
         * is implemented to update client side high water mark.
         */
        protected class ProducerCallbacks extends BaseCallbacks {

            @Override
            public void applyTransaction(Transaction transaction) {
                // update client side high water mark
                if (transaction.transactionId - 1 == clientHighWaterMark) {
                    clientHighWaterMark = transaction.transactionId;
                }
            }
        }
    }

    private abstract static class PerformanceBase extends Cli {

        protected final AtomicLong startTime;

        protected int numActivePartitions;
        protected int txnSize;
        protected byte[] data;
        protected CountDownLatch allTxnReceived;
        protected WaltzClientConfig waltzClientConfig;

        private PerformanceBase(String[] args) {
            super(args);
            startTime = new AtomicLong(0);
        }

        /**
         * This is an abstract class implements {@link WaltzClientCallbacks}, which will be invoked by WaltzClient
         * to retrieve client high-water mark, and to update client high-water mark when transaction applied.
         */
        protected abstract class BaseCallbacks implements WaltzClientCallbacks {

            protected long clientHighWaterMark;

            private BaseCallbacks() {
                clientHighWaterMark = -1;
            }

            @Override
            public long getClientHighWaterMark(int partitionId) {
                return clientHighWaterMark;
            }

            @Override
            public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
            }
        }
    }

    /**
     * Return an object of {@code WaltzClientConfig} built from configuration file.
     * @param configFilePath the path to configuration file
     * @return WaltzClientConfig
     * @throws IOException
     */
    private static WaltzClientConfig getWaltzClientConfig(String configFilePath) throws IOException {
        Yaml yaml = new Yaml();
        try (FileInputStream in = new FileInputStream(configFilePath)) {
            Map<Object, Object> props = yaml.load(in);
            return new WaltzClientConfig(props);
        }
    }

    public static void testMain(String[] args) {
        new PerformanceCli(args, true).processCmd();
    }

    public static void main(String[] args) {
        new PerformanceCli(args, false).processCmd();
    }
}
