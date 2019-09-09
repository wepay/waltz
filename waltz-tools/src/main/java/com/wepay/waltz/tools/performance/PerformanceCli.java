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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

    private static final int PARTITION_ID = 0;
    private static final Random RANDOM_LOCK_GENERATOR = new Random();
    private static final String FULL_PATH_CONFIG_FILE = "full_path_config_file";
    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int BYTES_IN_MEGABYTE = 1024 * 1024;

    private PerformanceCli(String[] args, boolean useByTest) {
        super(args, useByTest, Arrays.asList(
                new Subcommand(RunProducers.NAME, RunProducers.DESCRIPTION, RunProducers::new),
                new Subcommand(RunConsumers.NAME, RunConsumers.DESCRIPTION, RunConsumers::new)
        ));
    }

    private static final class RunProducers extends PerformanceBase {
        private static final String NAME = "test-producers";
        private static final String DESCRIPTION = "Runs a producer performance test";

        private static final int LAMBDA = 1;
        private static final int EXTRA_TRANSACTION_PER_THREAD = 1;

        private int txnPerThread;
        private int numThread;
        private int avgInterval;
        private CountDownLatch allClientsReady;
        private CountDownLatch allMountComplete;

        private RunProducers(String[] args) {
            super(args);
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
            Option lockPoolSizeOption = Option.builder("l")
                    .longOpt("lock-pool-size")
                    .desc("Specify size of lock pool")
                    .hasArg()
                    .build();

            txnSizeOption.setRequired(true);
            txnPerThreadOption.setRequired(true);
            numThreadOption.setRequired(true);
            intervalOption.setRequired(true);
            lockPoolSizeOption.setRequired(false);

            options.addOption(txnSizeOption);
            options.addOption(txnPerThreadOption);
            options.addOption(numThreadOption);
            options.addOption(intervalOption);
            options.addOption(lockPoolSizeOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                // check if full path config file provided
                if (cmd.getArgList().size() == 0) {
                    throw new IllegalArgumentException("Missing required argument: <full_path_config_file>");
                }
                this.clientConfig = getWaltzClientConfig(cmd.getArgList().get(0));
                txnSize = Integer.parseInt(cmd.getOptionValue("txn-size"));
                if (txnSize < 0) {
                    throw new IllegalArgumentException("Found negative: txn-size must be equal or greater than 0");
                }
                data = new byte[txnSize];

                // check required arguments
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

                // check optional argument
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
                    WaltzClient client = new WaltzClient(callbacks, clientConfig);
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

        @Override
        protected String getUsage() {
            List<String> descriptions = new ArrayList<>();
            descriptions.add("This is a CLI tool designed to analysis performance of Waltz producer. "
                    + "Each printout \"+\" represent a transaction success. "
                    + "Each printout \"_\" represent a transaction rejection.");
            descriptions.add("As a result, statistic will be collected and printed including Transaction/sec, MB/sec, "
                    + "Retry/Transaction, milliSec/Transaction and etc.");
            descriptions.add("lockPoolSize is optional. If not provide or equals to 0, no transaction will be rejected."
                    + "Otherwise, greater the lockPoolSize, less likely transactions get rejected.");
            return buildUsage(descriptions, getOptions(), FULL_PATH_CONFIG_FILE);
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
            sb.append(String.format("%nAppended %d transactions, with total %.4f MB, in %.2f secs%n", totalTxnSent, (1.0 * totalBytesSent / BYTES_IN_MEGABYTE), duration));
            sb.append(String.format("Transaction/sec: %.4f%n", (1.0 * totalTxnSent / duration)));
            sb.append(String.format("MB/sec: %.4f%n", totalBytesSent / duration / BYTES_IN_MEGABYTE));
            sb.append(String.format("%nRetry/Transaction: %.4f%n", (1.0 * totalRetry / totalTxnSent)));
            sb.append(String.format("milliSec/Transaction: %.4f%n", (1.0 * totalElapseMilliSecs / totalTxnSent)));
            System.out.println(sb.toString());
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
                        client.submit(new DummyTxnContext(data, false));

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
        private static final String DESCRIPTION = "Runs a consumer performance test";

        private int numTxn;
        private int producerClientId;
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

            txnSizeOption.setRequired(true);
            numTxnOption.setRequired(true);

            options.addOption(txnSizeOption);
            options.addOption(numTxnOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                // check if full path config file provided
                if (cmd.getArgList().size() == 0) {
                    throw new IllegalArgumentException("Missing required argument: <" + FULL_PATH_CONFIG_FILE + ">");
                }
                this.clientConfig = getWaltzClientConfig(cmd.getArgList().get(0));
                txnSize = Integer.parseInt(cmd.getOptionValue("txn-size"));
                if (txnSize < 0) {
                    throw new IllegalArgumentException("Found negative: txn-size must be equal or greater than 0");
                }
                data = new byte[txnSize];

                // check required argument
                numTxn = Integer.parseInt(cmd.getOptionValue("num-txn"));
                if (numTxn < 0) {
                    throw new IllegalArgumentException("Found negative: num-txn must be greater or equal to 0");
                }

                allTxnReceived = new CountDownLatch(numTxn);
                allTxnRead = new CountDownLatch(numTxn);

                // start thread
                new ConsumerThread(clientConfig).run();

                // wait until all transaction consumed
                allTxnRead.await();

                printStatistic();
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to run consumer performance test: %s", e.getMessage()));
            }
        }

        @Override
        protected String getUsage() {
            List<String> descriptions = new ArrayList<>();
            descriptions.add("This is a CLI tool designed to analysis performance of Waltz consumer. "
                    + "Each printout \"+\" represent a transaction success.");
            descriptions.add("As a result, statistic will be collected and print including Transaction/sec, MB/sec, "
                    + "milliSec/Transaction and etc.");
            return buildUsage(descriptions, getOptions());
        }

        private void printStatistic() {
            long totalElapseMilliSecs = totalResponseTimeMilli.get();
            long totalBytesRead = numTxn * txnSize;
            double duration = 1.0 * (System.currentTimeMillis() - startTime.get()) / MILLISECONDS_IN_SECOND;

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%n%nRead %d transactions, with total %.4f MB, in %.2f secs%n", numTxn, (1.0 * totalBytesRead / BYTES_IN_MEGABYTE), duration));
            sb.append(String.format("Transaction/sec: %.4f%n", (1.0 * numTxn / duration)));
            sb.append(String.format("MB/sec: %.4f%n", totalBytesRead / duration / BYTES_IN_MEGABYTE));
            sb.append(String.format("milliSec/Transaction: %.4f%n", (1.0 * totalElapseMilliSecs / numTxn)));
            System.out.println(sb.toString());
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

            private WaltzClient producer;
            private WaltzClient consumer;
            private WaltzClientConfig config;

            ConsumerThread(WaltzClientConfig config) {
                this.config = config;
            }

            @Override
            public void run() {
                try {
                    producer = new WaltzClient(new ProducerCallbacks(), config);
                    producerClientId = producer.clientId();
                    for (int j = 0; j < numTxn; j++) {
                        producer.submit(new DummyTxnContext(data, false));
                    }

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
                        producer.close();
                        consumer.close();
                        throw new SubCommandFailedException(ex);
                    } finally {
                        producer.close();
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
                if (transaction.reqId.clientId() == producerClientId) {
                    // start timer when first callback from producer client is received
                    startTime.compareAndSet(0, System.currentTimeMillis());

                    // read transaction data
                    transaction.getTransactionData(DummySerializer.INSTANCE);
                    allTxnRead.countDown();
                }
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
        protected final AtomicLong totalResponseTimeMilli;
        protected final Set<Integer> toMountClients;

        protected int lockPoolSize;
        protected int txnSize;
        protected int totalRetry;
        protected byte[] data;
        protected CountDownLatch allTxnReceived;
        protected WaltzClientConfig clientConfig;

        private PerformanceBase(String[] args) {
            super(args);
            startTime = new AtomicLong(0);
            totalResponseTimeMilli = new AtomicLong(0);
            toMountClients = new HashSet<>();
        }

        /**
         * This class extends {@link TransactionContext}, which encapsulates code to build a dummy transaction
         * with specific size of data.
         */
        protected final class DummyTxnContext extends TransactionContext {

            private static final String LOCK_NAME = "performance_analysis_lock";
            private final long timestampMilliSec;
            private final byte[] data;
            private final boolean ignoreLatency;
            private int execCount = 0;

            DummyTxnContext(byte[] data, boolean ignoreLatency) {
                this.timestampMilliSec = System.currentTimeMillis();
                this.data = data;
                this.ignoreLatency = ignoreLatency;
            }

            @Override
            public int partitionId(int numPartitions) {
                return PARTITION_ID;
            }

            @Override
            public boolean execute(TransactionBuilder builder) {
                if (execCount++ > 0) {
                    totalRetry++;
                }
                // Write dummy data
                builder.setTransactionData(data, DummySerializer.INSTANCE);
                if (lockPoolSize != 0) {
                    int lockId = RANDOM_LOCK_GENERATOR.nextInt(lockPoolSize);
                    builder.setWriteLocks(Collections.singletonList(new PartitionLocalLock(LOCK_NAME, lockId)));
                }
                return true;
            }

            @Override
            public void onCompletion(boolean result) {
                if (!ignoreLatency) {
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
