package com.wepay.waltz.tools.client;

import com.wepay.waltz.client.PartitionLocalLock;
import com.wepay.waltz.client.Serializer;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.TransactionBuilder;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientCallbacks;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.common.util.Cli;
import com.wepay.waltz.common.util.SubcommandCli;
import com.wepay.waltz.exception.SubCommandFailedException;
import com.wepay.waltz.tools.CliUtils.DummyTxnCallbacks;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Math.toIntExact;

/**
 * {@code ClientCli} is a tool designed to help integration test
 * , which utilizes Waltz Client to produce, consume transactions
 * and to validate system consistency and stability.
 */
public final class ClientCli extends SubcommandCli {

    private ClientCli(String[] args,  boolean useByTest) {
        super(args, useByTest, Arrays.asList(
                new Subcommand(Validate.NAME, Validate.DESCRIPTION, Validate::new),
                new Subcommand(HighWaterMark.NAME, HighWaterMark.DESCRIPTION, HighWaterMark::new)
        ));
    }

    /**
     * Use {@code Validate} command to submit transactions to Waltz
     * server for testing, and consume the transactions for validation,
     * including transaction data and optimistic locking.
     *
     * When running {@code Validate} command, there should not be any external
     * client that is producing TXNs of the same partition (different partition
     * is okay). Otherwise, the validation result will be unpredictable.
     *
     * <pre>
     * How validation is done:
     * 1. A number of clients work as producer to fire TXNs of given partition
     * 2. TXN data is always set to the client's (high watermark + 1)
     * 3. All TXNs to submit are set with the same lock
     * 4. Then, a client works as consumer will receive and count TXN callbacks
     * 5. The consumer will also check TXN data to validate optimistic locking
     *
     * With lock, no two TXN with same "data" will be appended, because they
     * hold the same high water mark, one gets blocked.
     * __________________________________________________________
     * server high water mark       txn data     submit client id
     * 0                            0            1
     * 1                            1            2
     * 2                            2            2
     *
     * Without lock, this may happen
     * __________________________________________________________
     * server high water mark       txn data     submit client id
     * 0                            0            1
     * 1                            1            2
     * 2                            1            1
     * </pre>
     */
    private static final class Validate extends Cli {
        private static final String NAME = "validate";
        private static final String DESCRIPTION = "Submit transactions for validation";
        private static final String LOCK_NAME = "validate-lock";
        private static final int LOCK_ID = 0;
        private static final int LAMBDA = 1;
        private static final int DEFAULT_NUMBER_ACTIVE_PARTITIONS = 1;
        private static final Random RANDOM = new Random();

        private static final List<PartitionLocalLock> LOCKS = Collections.singletonList(new PartitionLocalLock(LOCK_NAME, LOCK_ID));

        private final Map<Integer, ConcurrentHashMap<Integer, Long>> clientHighWaterMarkMap;
        private final List<String> uncaughtExceptions;

        private CountDownLatch allProducerReady;
        private CountDownLatch allProducerTxnCallbackReceived;
        private CountDownLatch allConsumerTxnCallbackReceived;

        private Validate(String[] args) {
            super(args);
            clientHighWaterMarkMap = new HashMap<>();
            uncaughtExceptions = Collections.synchronizedList(new ArrayList<>());
        }

        @Override
        protected void configureOptions(Options options) {
            Option txnPerClientOption = Option.builder("tpc")
                    .longOpt("txn-per-client")
                    .desc("Specify number of transactions per client")
                    .hasArg()
                    .build();
            Option numClientsOption = Option.builder("nc")
                    .longOpt("num-clients")
                    .desc("Specify number of total clients")
                    .hasArg()
                    .build();
            Option intervalOption = Option.builder("i")
                    .longOpt("interval")
                    .desc("Specify average interval(millisecond) between transactions")
                    .hasArg()
                    .build();
            Option cfgPathOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify client cli config file path")
                    .hasArg()
                    .build();
            Option numActivePartitionOption = Option.builder("ap")
                    .longOpt("num-active-partitions")
                    .desc(String.format("Specify number of partitions to interact with. e.g. if set to 3, transactions will"
                            + "be evenly distributed among partition 0, 1 and 2. Default to %d", DEFAULT_NUMBER_ACTIVE_PARTITIONS))
                    .hasArg()
                    .build();

            txnPerClientOption.setRequired(true);
            numClientsOption.setRequired(true);
            intervalOption.setRequired(true);
            cfgPathOption.setRequired(true);
            numActivePartitionOption.setRequired(false);

            options.addOption(txnPerClientOption);
            options.addOption(numClientsOption);
            options.addOption(intervalOption);
            options.addOption(cfgPathOption);
            options.addOption(numActivePartitionOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                // check required arguments
                int txnPerClient = Integer.parseInt(cmd.getOptionValue("txn-per-client"));
                if (txnPerClient < 0) {
                    throw new IllegalArgumentException("Found negative: txn-per-client must be greater or equals to 0");
                }
                int numClients = Integer.parseInt(cmd.getOptionValue("num-clients"));
                if (numClients < 0) {
                    throw new IllegalArgumentException("Found negative: num-clients must be greater or equals to 0");
                }
                int avgInterval = Integer.parseInt(cmd.getOptionValue("interval"));
                if (avgInterval < 0) {
                    throw new IllegalArgumentException("Found negative: interval must be greater or equals to 0");
                }
                String configFilePath = cmd.getOptionValue("cli-config-path");
                WaltzClientConfig waltzClientConfig = getWaltzClientConfig(configFilePath);

                // check optional argument
                int numActivePartitions = cmd.hasOption("num-active-partitions") ? Integer.parseInt(cmd.getOptionValue("num-active-partitions")) : DEFAULT_NUMBER_ACTIVE_PARTITIONS;
                if (numActivePartitions < 1) {
                    throw new IllegalArgumentException("num-active-partitions must be greater or equals to 1");
                }

                // get number of existing transactions across all partitions
                long numTxnToSubmit = txnPerClient * numClients;
                long numExistingTransactions = 0;
                for (int partitionId = 0; partitionId < numActivePartitions; partitionId++) {
                    long partitionHighWaterMark = getHighWaterMark(partitionId, waltzClientConfig);
                    numExistingTransactions += partitionHighWaterMark > -1L ? partitionHighWaterMark + 1 : 0;
                }

                // each client will receive callback of all transactions
                int expectNumProducerCallbacks = toIntExact((numExistingTransactions + numTxnToSubmit) * numClients);
                int expectNumConsumerCallbacks = toIntExact((numExistingTransactions + numTxnToSubmit) * 1);
                allProducerReady = new CountDownLatch(numClients);
                allProducerTxnCallbackReceived = new CountDownLatch(expectNumProducerCallbacks);
                allConsumerTxnCallbackReceived = new CountDownLatch(expectNumConsumerCallbacks);

                produceTransactions(numActivePartitions, numClients, txnPerClient, avgInterval, waltzClientConfig);

                consumeAndValidate(waltzClientConfig);

                checkUncaughtExceptions();
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Transaction validation failed: %s", e.getMessage()));
            }

        }

        private void checkUncaughtExceptions() {
            if (!uncaughtExceptions.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (String errorMsg: uncaughtExceptions) {
                    sb.append(errorMsg);
                }
                throw new SubCommandFailedException(sb.toString());
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }

        /**
         * Produce specific number of transactions with each client with intervals.
         * @param numActivePartitions number of active partitions
         * @param numClients number of clients
         * @param txnPerClient number of transactions to submit for each clients
         * @param avgInterval average submission interval
         * @param config WaltzClientConfig
         * @throws Exception
         */
        private void produceTransactions(int numActivePartitions, int numClients, int txnPerClient, int avgInterval,
                                         WaltzClientConfig config) throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(numClients);
            for (int i = 0; i < numClients; i++) {
                ProducerTxnCallbacks producerTnxCallback = new ProducerTxnCallbacks();
                WaltzClient producer = new WaltzClient(producerTnxCallback, config);

                // set producerTnxCallback.clientId, so that will only update
                // that producer's high-watermark each callback
                producerTnxCallback.clientId = producer.clientId();
                clientHighWaterMarkMap.putIfAbsent(producer.clientId(), new ConcurrentHashMap<>());

                // all thread start, but transactions won't be fired until all clients are ready
                executor.execute(new ProducerThread(numActivePartitions, txnPerClient, avgInterval, producer));
                allProducerReady.countDown();
            }

            allProducerTxnCallbackReceived.await();
            executor.shutdown();
        }

        /**
         * Consume all transactions, and validate callbacks of all partitions.
         * @param config WaltzClientConfig
         * @throws Exception
         */
        private void consumeAndValidate(WaltzClientConfig config) throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(1);
            ConsumerTxnCallbacks consumerTxnCallback = new ConsumerTxnCallbacks();
            WaltzClient consumer = new WaltzClient(consumerTxnCallback, config);

            // set consumerTxnCallback.clientId, so that will only update
            // that consumer's high-watermark each callback
            consumerTxnCallback.clientId = consumer.clientId();
            clientHighWaterMarkMap.putIfAbsent(consumer.clientId(), new ConcurrentHashMap<>());
            executor.execute(new ConsumerThread(consumer));

            allConsumerTxnCallbackReceived.await();
            executor.shutdown();
        }

        /**
         * This class implements {@link Runnable}, which waits for all clients get ready, iteratively fires all
         * transactions with an exponential distributed interval. Transactions will be evenly distributed among
         * all active partitions.
         */
        private final class ProducerThread implements Runnable {
            private int numActivePartitions;
            private int txnPerThread;
            private int avgInterval;
            private WaltzClient client;

            ProducerThread(int numActivePartitions, int txnPerThread, int avgInterval, WaltzClient client) {
                this.numActivePartitions = numActivePartitions;
                this.txnPerThread = txnPerThread;
                this.avgInterval = avgInterval;
                this.client = client;
            }

            @Override
            public void run() {
                try {
                    allProducerReady.await();

                    // fire transactions
                    for (int j = 0; j < txnPerThread; j++) {
                        int partitionId = RANDOM.nextInt(numActivePartitions);
                        client.submit(new HighWaterMarkTxnContext(partitionId, client.clientId()));

                        // By adjusting the distribution parameter of the random sleep,
                        // we can test various congestion scenarios.
                        Thread.sleep(nextExponentialDistributedInterval(avgInterval));
                    }
                    allProducerTxnCallbackReceived.await();
                } catch (InterruptedException ex) {
                    client.close();
                    throw new SubCommandFailedException(ex);
                } finally {
                    client.close();
                }
            }
        }

        /**
         * This class implements {@link Runnable}, which consumes and validate previous
         * transactions.
         */
        private class ConsumerThread implements Runnable {
            private WaltzClient client;

            ConsumerThread(WaltzClient client) {
                this.client = client;
            }

            @Override
            public void run() {
                try {
                    allConsumerTxnCallbackReceived.await();
                } catch (InterruptedException ex) {
                    client.close();
                    throw new SubCommandFailedException(ex);
                } finally {
                    client.close();
                }
            }
        }

        /**
         * This class extends {@link WaltzClientCallbacks}, which updates
         * client side high water mark for each callback.
         */
        private final class ProducerTxnCallbacks implements WaltzClientCallbacks {
            private static final int DEFAULT_CLIENT_ID = -1;

            // clientId will be reset with consumer.clientId()
            private int clientId = DEFAULT_CLIENT_ID;

            private ProducerTxnCallbacks() {
            }

            @Override
            public long getClientHighWaterMark(int partitionId) {
                if (clientId == DEFAULT_CLIENT_ID) {
                    return -1;
                }
                return clientHighWaterMarkMap.get(clientId).getOrDefault(partitionId, -1L);
            }

            @Override
            public void applyTransaction(Transaction transaction) {
                // update client side high water mark when partition Id matches
                int partitionId = transaction.reqId.partitionId();
                long curHighWaterMark = clientHighWaterMarkMap.get(clientId).getOrDefault(partitionId, -1L);
                if (transaction.transactionId == curHighWaterMark + 1) {
                    clientHighWaterMarkMap.get(clientId).put(partitionId, transaction.transactionId);
                    allProducerTxnCallbackReceived.countDown();
                } else {
                    throw new SubCommandFailedException(String.format("expect callback transaction id to be %s, but got %s",
                                                                      curHighWaterMark + 1, transaction.transactionId));
                }
            }

            @Override
            public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
                uncaughtExceptions.add(String.format("UncaughtException[partition:%d, transactionId:%d]: %s%n",
                                                     partitionId, transactionId, exception.getMessage()));
            }
        }

        /**
         * This class extends {@link WaltzClientCallbacks}, which validate
         * and countdown number of callbacks received by consumer.
         */
        private final class ConsumerTxnCallbacks implements WaltzClientCallbacks {
            private static final int DEFAULT_CLIENT_ID = -1;

            // clientId will be reset with consumer.clientId()
            private int clientId = DEFAULT_CLIENT_ID;

            private ConsumerTxnCallbacks() {
            }

            @Override
            public long getClientHighWaterMark(int partitionId) {
                if (clientId == DEFAULT_CLIENT_ID) {
                    return -1L;
                }
                return clientHighWaterMarkMap.get(clientId).getOrDefault(partitionId, -1L);
            }

            @Override
            public void applyTransaction(Transaction transaction) {
                int partitionId = transaction.reqId.partitionId();
                long curHighWaterMark = clientHighWaterMarkMap.get(clientId).getOrDefault(partitionId, -1L);
                if (transaction.transactionId == curHighWaterMark + 1) {
                    if (transaction.getTransactionData(HighWaterMarkSerializer.INSTANCE).longValue() != transaction.transactionId) {
                        throw new SubCommandFailedException("optimistic locking validation failed");
                    }
                    // update client side high water mark
                    clientHighWaterMarkMap.get(clientId).put(partitionId, transaction.transactionId);
                    allConsumerTxnCallbackReceived.countDown();
                } else {
                    throw new SubCommandFailedException(String.format("expect callback transaction id to be %s, but got %s",
                                                                      curHighWaterMark + 1, transaction.transactionId));
                }
            }

            @Override
            public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
                uncaughtExceptions.add(String.format("UncaughtException[partition:%d, transactionId:%d]: %s%n",
                                                     partitionId, transactionId, exception.getMessage()));
            }
        }

        /**
         * This class extends {@link TransactionContext}, which builds a transaction
         * that uses current high water mark to set transaction data.
         */
        private final class HighWaterMarkTxnContext extends TransactionContext {
            private final int partitionId;
            private final int clientId;

            HighWaterMarkTxnContext(int partitionId, int clientId) {
                this.partitionId = partitionId;
                this.clientId = clientId;
            }

            @Override
            public int partitionId(int numPartitions) {
                return partitionId;
            }

            @Override
            public boolean execute(TransactionBuilder builder) {
                long clientHighWaterMark = clientHighWaterMarkMap.get(clientId).getOrDefault(partitionId, -1L);
                builder.setTransactionData(clientHighWaterMark + 1, HighWaterMarkSerializer.INSTANCE);
                builder.setWriteLocks(LOCKS);
                return true;
            }
        }

        private static final class HighWaterMarkSerializer implements Serializer<Long> {
            private static final HighWaterMarkSerializer INSTANCE = new HighWaterMarkSerializer();

            @Override
            public byte[] serialize(Long data) {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(data);
                return buffer.array();
            }

            @Override
            public Long deserialize(byte[] bytes) {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.put(bytes);
                buffer.flip();
                return buffer.getLong();
            }
        }

        /**
         * Return next interval between transactions of a client, so that transaction events follow poisson distribution.
         *
         * @return next exponential distributed interval in millisecond.
         */
        private static int nextExponentialDistributedInterval(int avgInterval) {
            // LAMBDA defaults to 1, so average interval is decided by avgInterval
            return (int) (avgInterval * (Math.log(1 - Math.random()) / -LAMBDA));
        }
    }

    /**
     * The {@code MaxTransactionId} command displays the maximum transaction ID of given partition.
     */
    private static final class HighWaterMark extends Cli {
        private static final String NAME = "high-water-mark";
        private static final String DESCRIPTION = "Displays high water mark of given partition";

        private HighWaterMark(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option partitionOption = Option.builder("p")
                    .longOpt("partition")
                    .desc("Specify the partition id whose max transaction ID to be returned")
                    .hasArg()
                    .build();
            Option cliCfgOption = Option.builder("c")
                    .longOpt("cli-config-path")
                    .desc("Specify the cli config file path required for zooKeeper connection string, zooKeeper root path and SSL config")
                    .hasArg()
                    .build();
            partitionOption.setRequired(true);
            cliCfgOption.setRequired(true);

            options.addOption(partitionOption);
            options.addOption(cliCfgOption);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            String partitionId = cmd.getOptionValue("partition");
            String configFilePath = cmd.getOptionValue("cli-config-path");
            try {
                WaltzClientConfig waltzClientConfig = getWaltzClientConfig(configFilePath, false);
                long highWaterMark = getHighWaterMark(Integer.parseInt(partitionId), waltzClientConfig);
                System.out.println(String.format("Partition %s current high watermark: %d", partitionId, highWaterMark));
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Failed to get high watermark of partition %s. %n%s", partitionId, e.getMessage()));
            }
        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }
    }

    /**
     * Return an object of {@code WaltzClientConfig} built from configuration file.
     * @param configFilePath the path to configuration file
     * @return WaltzClientConfig
     * @throws IOException
     */
    private static WaltzClientConfig getWaltzClientConfig(String configFilePath) throws IOException {
        return getWaltzClientConfig(configFilePath, WaltzClientConfig.DEFAULT_AUTO_MOUNT);
    }

    /**
     * Return an object of {@code WaltzClientConfig} built from configuration file.
     * @param configFilePath the path to configuration file
     * @param autoMount if set to false, partitions will not be mounted or receive feed
     * @return WaltzClientConfig
     * @throws IOException
     */
    private static WaltzClientConfig getWaltzClientConfig(String configFilePath, boolean autoMount) throws IOException {
        Yaml yaml = new Yaml();
        try (FileInputStream in = new FileInputStream(configFilePath)) {
            Map<Object, Object> props = yaml.load(in);
            props.put(WaltzClientConfig.AUTO_MOUNT, autoMount);
            return new WaltzClientConfig(props);
        }
    }

    private static long getHighWaterMark(int partitionId, WaltzClientConfig config) throws Exception {
        DummyTxnCallbacks callbacks = new DummyTxnCallbacks();
        WaltzClient client = new WaltzClient(callbacks, config);
        return client.getHighWaterMark(partitionId);
    }

    public static void testMain(String[] args) {
        new ClientCli(args, true).processCmd();
    }

    public static void main(String[] args) {
        new ClientCli(args, false).processCmd();
    }

}
