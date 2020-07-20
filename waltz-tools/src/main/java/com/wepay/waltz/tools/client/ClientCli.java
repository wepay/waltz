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
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.stream.Collectors;


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
                new Subcommand(HighWaterMark.NAME, HighWaterMark.DESCRIPTION, HighWaterMark::new),
                new Subcommand(Producer.NAME, Producer.DESCRIPTION, Producer::new),
                new Subcommand(Consumer.NAME, Consumer.DESCRIPTION, Consumer::new),
                new Subcommand(ClientProcessesSetup.NAME, ClientProcessesSetup.DESCRIPTION, ClientProcessesSetup::new)
        ));
    }

    /**
     * Use {@code ClientProcessesSetup} command to submit transactions to Waltz
     * server for testing, and consume the transactions for validation,
     * including transaction data and optimistic locking. Unlike when running
     * {@code Validate} every single client (producer & consumer) is an independent
     * process created by firing {@code Producer} and {@code Consumer} command.
     *
     * When running {@code ClientProcessesSetup} command, there should not be any external
     * client that is producing TXNs of the same partition (different partition
     * is okay). Otherwise, the validation result will be unpredictable. Validation of every single
     * client is done the same way as in {@code Validate}. Main thread waits for all processes to finish
     * and checks their input stream for validation.
     */
    private static final class ClientProcessesSetup extends Cli {
        private static final String NAME = "client-processes-setup";
        private static final String DESCRIPTION = "Creates multiple processes of producers and consumers";
        private static final int DEFAULT_NUMBER_ACTIVE_PARTITIONS = 1;
        private static final String DEFAULT_Dlog4j_CONFIG_PATH = "/etc/waltz-client/waltz-log4j.cfg";

        private ClientProcessesSetup(String[] args) {
            super(args);
        }

        @Override
        protected void configureOptions(Options options) {
            Option txnPerProducerOption = Option.builder("tpp")
                    .longOpt("txn-per-producer")
                    .desc("Specify number of transactions per producer")
                    .hasArg()
                    .build();
            Option numProducersOption = Option.builder("np")
                    .longOpt("num-producers")
                    .desc("Specify number of total producers")
                    .hasArg()
                    .build();
            Option numConsumersOption = Option.builder("nc")
                    .longOpt("num-consumers")
                    .desc("Specify number of total consumers")
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
            Option previousHighWaterMarkValue = Option.builder("phw")
                    .longOpt("previous-high-watermark")
                    .desc("Specify what was the high watermark before this test setup. "
                            + "Expected format for 4 active partitions is as follows: \"-1 -1 42 -1\"")
                    .hasArg()
                    .build();
            Option dlog4jConfigurationPath = Option.builder("d4j")
                    .longOpt("dlog4j-configuration-path")
                    .desc("Specify dlog4j configuration path, default: /etc/waltz-client/waltz-log4j.cfg")
                    .hasArg()
                    .build();

            txnPerProducerOption.setRequired(true);
            numProducersOption.setRequired(true);
            numConsumersOption.setRequired(true);
            intervalOption.setRequired(true);
            cfgPathOption.setRequired(true);
            numActivePartitionOption.setRequired(false);
            previousHighWaterMarkValue.setRequired(false);
            dlog4jConfigurationPath.setRequired(false);

            options.addOption(txnPerProducerOption);
            options.addOption(numProducersOption);
            options.addOption(numConsumersOption);
            options.addOption(intervalOption);
            options.addOption(cfgPathOption);
            options.addOption(numActivePartitionOption);
            options.addOption(previousHighWaterMarkValue);
            options.addOption(dlog4jConfigurationPath);
        }

        @Override
        protected void processCmd(CommandLine cmd) throws SubCommandFailedException {
            try {
                ArrayList<Process> clients = new ArrayList<>();
                String[] producerString = createProducerString(cmd);
                String[] consumerString = createConsumerString(cmd);

                int numOfProducers = Integer.parseInt(cmd.getOptionValue("num-producers"));
                int numOfConsumers = Integer.parseInt(cmd.getOptionValue("num-consumers"));

                //Start Producers
                for (int i = 0; i < numOfProducers; i++) {
                    Process p = Runtime.getRuntime().exec(producerString);
                    clients.add(p);
                }
                //Start Consumers
                for (int i = 0; i < numOfConsumers; i++) {
                    Process p = Runtime.getRuntime().exec(consumerString);
                    clients.add(p);
                }
                //Wait till all clients finish
                for (Process p : clients) {
                    p.waitFor();
                    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), "UTF-8"));
                    String output = br.lines().collect(Collectors.joining());
                    br.close();
                    if (!output.contains("success")) {
                        throw new Exception("At least one client crashed");
                    }
                }
            }  catch (Exception e) {
                throw new SubCommandFailedException(String.format("Transaction validation failed: %s", e.getMessage()));
            }

        }

        @Override
        protected String getUsage() {
            return buildUsage(NAME, DESCRIPTION, getOptions());
        }

        private String[] createProducerString(CommandLine cmd) throws SubCommandFailedException {
            String cfgPath = cmd.hasOption("dlog4j-configuration-path") ? cmd.getOptionValue("dlog4j-configuration-path") : DEFAULT_Dlog4j_CONFIG_PATH;
            String numOfActivePartitions = cmd.hasOption("num-active-partitions") ? cmd.getOptionValue("num-active-partitions") : String.valueOf(DEFAULT_NUMBER_ACTIVE_PARTITIONS);

            return new String[]{
                "java", "-cp", "/usr/local/waltz/waltz-uber.jar", "-Dlog4j.configuration=" + cfgPath,
                "com.wepay.waltz.tools.client.ClientCli", "create-producer",
                "--txn-per-client", cmd.getOptionValue("txn-per-producer"),
                "--num-clients", cmd.getOptionValue("num-producers"),
                "--cli-config-path", cmd.getOptionValue("cli-config-path"),
                "--num-active-partitions", numOfActivePartitions,
                "--previous-high-watermark", cmd.getOptionValue("previous-high-watermark"),
                "--interval", cmd.getOptionValue("interval")
            };
        }

        private String[] createConsumerString(CommandLine cmd) throws SubCommandFailedException {
            String cfgPath = cmd.hasOption("dlog4j-configuration-path") ? cmd.getOptionValue("dlog4j-configuration-path") : DEFAULT_Dlog4j_CONFIG_PATH;
            String numOfActivePartitions = cmd.hasOption("num-active-partitions") ? cmd.getOptionValue("num-active-partitions") : String.valueOf(DEFAULT_NUMBER_ACTIVE_PARTITIONS);

            return new String[]{
                    "java", "-cp", "/usr/local/waltz/waltz-uber.jar", "-Dlog4j.configuration=" + cfgPath,
                    "com.wepay.waltz.tools.client.ClientCli", "create-consumer",
                    "--txn-per-client", cmd.getOptionValue("txn-per-producer"),
                    "--num-clients", cmd.getOptionValue("num-producers"),
                    "--cli-config-path", cmd.getOptionValue("cli-config-path"),
                    "--num-active-partitions", numOfActivePartitions,
                    "--previous-high-watermark", cmd.getOptionValue("previous-high-watermark")
            };
        }
    }

    /**
     * Use {@code Consumer} command to create a consumer client that
     * reads transactions submitted to Waltz server for validation,
     * including transaction data and optimistic locking.
     *
     * Consumer process is closed once total number of processed transactions
     * reaches number of transactions already stored (previous-high-watermark)
     * plus txn-per-client * num-clients (number of producing clients).
     */
    private static final class Consumer extends Validate {
        private static final String NAME = "create-consumer";
        private static final String DESCRIPTION = "Creates one consumer";
        private static final int DEFAULT_NUMBER_ACTIVE_PARTITIONS = 1;
        private static final String[] DEFAULT_HIGH_WATERMARK_VALUE = {"-1"};

        private Consumer(String[] args) {
            super(args);
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
            Option previousHighWaterMarkValue = Option.builder("phw")
                    .longOpt("previous-high-watermark")
                    .desc("Specify what was the high watermark before this test setup. "
                            + "Expected format for 4 active partitions is as follows: \"-1 -1 42 -1\"")
                    .hasArg()
                    .build();

            txnPerClientOption.setRequired(true);
            numClientsOption.setRequired(true);
            cfgPathOption.setRequired(true);
            numActivePartitionOption.setRequired(false);
            previousHighWaterMarkValue.setRequired(false);

            options.addOption(txnPerClientOption);
            options.addOption(numClientsOption);
            options.addOption(cfgPathOption);
            options.addOption(numActivePartitionOption);
            options.addOption(previousHighWaterMarkValue);
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
                String configFilePath = cmd.getOptionValue("cli-config-path");
                WaltzClientConfig waltzClientConfig = getWaltzClientConfig(configFilePath);

                // check optional arguments
                int numActivePartitions = cmd.hasOption("num-active-partitions") ? Integer.parseInt(cmd.getOptionValue("num-active-partitions")) : DEFAULT_NUMBER_ACTIVE_PARTITIONS;
                if (numActivePartitions < 1) {
                    throw new IllegalArgumentException("num-active-partitions must be greater or equals to 1");
                }
                String[] prevHighWaterMark = cmd.hasOption("previous-high-watermark") ? cmd.getOptionValue("previous-high-watermark").split(" ") : DEFAULT_HIGH_WATERMARK_VALUE;

                // get number of existing transactions across all partitions
                long numTxnToSubmit = txnPerClient * numClients;
                long numExistingTransactions = 0;
                if (numActivePartitions != prevHighWaterMark.length) {
                    throw new IllegalArgumentException("previous high-watermark must be a list of values corresponding to number of active partitions "
                            + "with all numbers greater or equal to -1");
                }
                for (int partitionId = 0; partitionId < numActivePartitions; partitionId++) {
                    long partitionHighWaterMark = Long.parseLong(prevHighWaterMark[partitionId]);
                    numExistingTransactions += partitionHighWaterMark > -1L ? partitionHighWaterMark + 1 : 0;
                }

                // each client will receive callback of all transactions
                int expectNumConsumerCallbacks = toIntExact(numExistingTransactions + numTxnToSubmit);
                allConsumerTxnCallbackReceived = new CountDownLatch(expectNumConsumerCallbacks);

                consumeAndValidate(waltzClientConfig);

                checkUncaughtExceptions();
                System.out.println("success");
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Transaction validation failed: %s", e.getMessage()));
            }
        }
    }

    /**
     * Use {@code Producer} command to create a producer client that
     * submits transactions to Waltz server.
     *
     * Producer process is closed once total sum of high watermarks for active
     * partitions reaches number of transactions already stored (previous-high-watermark)
     * plus txn-per-client * num-clients (number of producing clients).
     */
    private static final class Producer extends Validate {
        private static final String NAME = "create-producer";
        private static final String DESCRIPTION = "Creates one producer";
        private static final int DEFAULT_NUMBER_ACTIVE_PARTITIONS = 1;
        private static final String[] DEFAULT_HIGH_WATERMARK_VALUE = {"-1"};

        private Producer(String[] args) {
            super(args);
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
            Option previousHighWaterMarkValue = Option.builder("phw")
                    .longOpt("previous-high-watermark")
                    .desc("Specify what was the high watermark before this test setup. "
                            + "Expected format for 4 active partitions is as follows: \"-1 -1 42 -1\"")
                    .hasArg()
                    .build();

            txnPerClientOption.setRequired(true);
            numClientsOption.setRequired(true);
            intervalOption.setRequired(true);
            cfgPathOption.setRequired(true);
            numActivePartitionOption.setRequired(false);
            previousHighWaterMarkValue.setRequired(false);

            options.addOption(txnPerClientOption);
            options.addOption(numClientsOption);
            options.addOption(intervalOption);
            options.addOption(cfgPathOption);
            options.addOption(numActivePartitionOption);
            options.addOption(previousHighWaterMarkValue);
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

                // check optional arguments
                int numActivePartitions = cmd.hasOption("num-active-partitions") ? Integer.parseInt(cmd.getOptionValue("num-active-partitions")) : DEFAULT_NUMBER_ACTIVE_PARTITIONS;
                if (numActivePartitions < 1) {
                    throw new IllegalArgumentException("num-active-partitions must be greater or equals to 1");
                }
                String[] prevHighWaterMark = cmd.hasOption("previous-high-watermark") ? cmd.getOptionValue("previous-high-watermark").split(" ") : DEFAULT_HIGH_WATERMARK_VALUE;
                // get number of existing transactions across all partitions
                long numTxnToSubmit = txnPerClient * numClients;
                long numExistingTransactions = 0;

                if (numActivePartitions != prevHighWaterMark.length) {
                    throw new IllegalArgumentException("previous-highwatermark must be a list of values corresponding to number of active partitions "
                            + "with all numbers greater or equal to -1");
                }
                for (int partitionId = 0; partitionId < numActivePartitions; partitionId++) {
                    long partitionHighWaterMark = Long.parseLong(prevHighWaterMark[partitionId]);
                    numExistingTransactions += partitionHighWaterMark > -1L ? partitionHighWaterMark + 1 : 0;
                }

                // each client will receive callback of all transactions
                int expectNumProducerCallbacks = toIntExact(numExistingTransactions + numTxnToSubmit);

                allProducerReady = new CountDownLatch(1);
                allProducerTxnCallbackReceived = new CountDownLatch(expectNumProducerCallbacks);

                produceTransactions(numActivePartitions, 1, txnPerClient, avgInterval, waltzClientConfig);

                checkUncaughtExceptions();
                System.out.println(" success");
            } catch (Exception e) {
                throw new SubCommandFailedException(String.format("Transaction validation failed: %s", e.getMessage()));
            }
        }
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
    protected static class Validate extends Cli {
        private static final String NAME = "validate";
        private static final String DESCRIPTION = "Submit transactions for validation";
        private static final String LOCK_NAME = "validate-lock";
        private static final int LOCK_ID = 0;
        private static final int LAMBDA = 1;
        private static final int DEFAULT_NUMBER_ACTIVE_PARTITIONS = 1;
        private static final Random RANDOM = new Random();

        private static final List<PartitionLocalLock> LOCKS = Collections.singletonList(new PartitionLocalLock(LOCK_NAME, LOCK_ID));

        protected final Map<Integer, ConcurrentHashMap<Integer, Long>> clientHighWaterMarkMap;
        protected final List<String> uncaughtExceptions;

        protected CountDownLatch allProducerReady;
        protected CountDownLatch allProducerTxnCallbackReceived;
        protected CountDownLatch allConsumerTxnCallbackReceived;

        protected Validate(String[] args) {
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

        protected void checkUncaughtExceptions() {
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
        protected void produceTransactions(int numActivePartitions, int numClients, int txnPerClient, int avgInterval,
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
        protected void consumeAndValidate(WaltzClientConfig config) throws Exception {
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
        WaltzClient client = null;
        try {
            DummyTxnCallbacks callbacks = new DummyTxnCallbacks();
            client = new WaltzClient(callbacks, config);
            return client.getHighWaterMark(partitionId);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    /**
     * A transaction callback to help construct {@link WaltzClient}. It is dummy because
     * it is not suppose to receive any callbacks.
     */
    private static final class DummyTxnCallbacks implements WaltzClientCallbacks {

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

    public static void testMain(String[] args) {
        new ClientCli(args, true).processCmd();
    }

    public static void main(String[] args) {
        new ClientCli(args, false).processCmd();
    }

}
