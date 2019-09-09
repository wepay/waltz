package com.wepay.waltz.demo;

import com.wepay.riff.config.AbstractConfig;
import com.wepay.waltz.client.AbstractClientCallbacksForJDBC;
import com.wepay.waltz.client.PartitionLocalLock;
import com.wepay.waltz.client.Transaction;
import com.wepay.waltz.client.TransactionBuilder;
import com.wepay.waltz.client.TransactionContext;
import com.wepay.waltz.client.WaltzClient;
import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.waltz.common.util.Cli;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public final class DemoBankApp extends DemoAppBase {

    private static final int INTERACTIVE_MODE = 1;
    private static final int AUTOMATIC_MODE = 2;

    private static final int ACCOUNT_ID = 1000000001;
    private static final int TEN_CENTS = 10;
    private static final double FIFTY_PERCENT = 0.5;
    private static final String CLIENT_HIGH_WATER_MARK_TABLE_NAME = "ACCOUNT_CLIENT_HIGH_WATER_MARK";

    private PrintStream out;
    private WaltzClientConfig waltzClientConfig;
    private int mode;

    public static void main(String[] args) throws Exception {
        // check optional config yaml file
        DemoBankAppCli cli = new DemoBankAppCli(args);
        cli.processCmd();

        DataSource dataSource;
        WaltzClientConfig waltzClientConfig;

        String configPath = cli.getConfigPath();
        if (configPath != null) {
            try (FileInputStream input = new FileInputStream(configPath)) {
                Yaml yaml = new Yaml();
                Map<Object, Object> props = yaml.load(input);
                DemoBankAppConfig config = new DemoBankAppConfig(props);
                String dataSourceConnectString = (String) config.get(DemoBankAppConfig.DS_CONNECT_STRING);
                String dataSourceUsername = (String) config.get(DemoBankAppConfig.DS_USERNAME);
                String dataSourcePassword = (String) config.get(DemoBankAppConfig.DS_PASSWORD);

                dataSource = new DemoDataSource(dataSourceConnectString, dataSourceUsername, dataSourcePassword);
                waltzClientConfig = new WaltzClientConfig(props);
            }
        } else {
            dataSource = new DemoDataSource(DemoConst.BANK_DB, "demo", "demo");
            waltzClientConfig = config();
        }

        try (DemoBankApp app = new DemoBankApp(dataSource, waltzClientConfig, System.out)) {
            app.setMode();
            app.run();
        }
    }

    DemoBankApp(DataSource dataSource, WaltzClientConfig waltzClientConfig, PrintStream out) throws SQLException {
        super(dataSource);

        try (Connection connection = dataSource.getConnection()) {
            createClientHighWaterMarkTable(connection, CLIENT_HIGH_WATER_MARK_TABLE_NAME);
            createAccountsTable(connection);
        }

        this.out = out;
        this.waltzClientConfig = waltzClientConfig;
        this.mode = INTERACTIVE_MODE;
    }

    private static WaltzClientConfig config() throws UnknownHostException {
        Properties props = new Properties();
        String host = InetAddress.getLocalHost().getCanonicalHostName();
        props.setProperty(WaltzClientConfig.ZOOKEEPER_CONNECT_STRING, host + ":" + DemoServers.ZK_PORT);
        props.setProperty(WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT, Integer.toString(DemoServers.ZK_SESSION_TIMEOUT));
        props.setProperty(WaltzClientConfig.CLUSTER_ROOT, DemoServers.CLUSTER_ROOT_ZNODE.toString());
        return new WaltzClientConfig(props);
    }

    private void setMode() {
        Scanner sc = new Scanner(System.in, "UTF-8");
        int inputMode;
        do {
            out.println("Choose run mode number: 1-[interactive] 2-[automatic]");
            if (!sc.hasNextInt()) {
                out.printf("\"%s\" is not a valid number, try again%n", sc.next());
            } else {
                inputMode = sc.nextInt();
                if (inputMode != INTERACTIVE_MODE && inputMode != AUTOMATIC_MODE) {
                    out.println("Invalid mode number, try again");
                } else {
                    mode = inputMode;
                    break;
                }
            }
        } while (sc.hasNext());
    }

    private void createAccountsTable(Connection connection) throws SQLException {
        executeSQL("DROP TABLE IF EXISTS ACCOUNTS",
            connection,
            true // ignore exception
        );
        executeSQL(
            "CREATE TABLE ACCOUNTS ("
                + "ACCOUNT_ID INTEGER NOT NULL,"
                + "BALANCE INTEGER NOT NULL,"
                + "ACTIVE CHAR(1) NOT NULL,"
                + "PRIMARY KEY (ACCOUNT_ID)"
                + ")",
            connection,
            false
        );
    }

    public void run() {
        try {
            initWaltzClient();

            if (mode == AUTOMATIC_MODE) {
                while (true) {
                    int amount = (int) Math.round(Math.random() * TEN_CENTS);
                    try {
                        if (Math.random() >= FIFTY_PERCENT) {
                            client.submit(new DemoBankApp.DepositTxnContext(ACCOUNT_ID, amount));
                        } else {
                            client.submit(new DemoBankApp.WithdrawalTxnContext(ACCOUNT_ID, amount, dataSource));
                        }
                    } catch (NumberFormatException ex) {
                        ex.printStackTrace();
                    }
                }
            } else { // mode == INTERACTIVE_MODE
                String prompt = "\nEnter [deposit <accountId> <amount>, withdraw <accountId> <amount>, "
                    + "balance <accountId>, deactivate <accountId>, quit] \n";
                String[] command = prompt(prompt);
                while (command != null && processCmd(command)) {
                    command = prompt(prompt);
                }
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    void initWaltzClient() throws Exception {
        DemoBankApp.DemoBankAppClientCallbacks callbacks = new DemoBankAppClientCallbacks(dataSource);
        client = new WaltzClient(callbacks, waltzClientConfig);
    }

    boolean processCmd(String... command) {
        if (command.length > 0) {
            switch (command[0]) {
                case "quit":
                    return false;

                case "deposit":
                    if (command.length == 3) {
                        try {
                            int accountId = Integer.parseInt(command[1]);
                            int amount = Integer.parseInt(command[2]);

                            client.submit(new DepositTxnContext(accountId, amount));

                        } catch (NumberFormatException ex) {
                            ex.printStackTrace();
                        }
                    } else {
                        out.println("not enough parameters");
                    }
                    return true;

                case "withdraw":
                    if (command.length == 3) {
                        try {
                            int accountId = Integer.parseInt(command[1]);
                            int amount = Integer.parseInt(command[2]);

                            client.submit(new WithdrawalTxnContext(accountId, amount, dataSource));

                        } catch (NumberFormatException ex) {
                            ex.printStackTrace();
                        }
                    } else {
                        out.println("not enough parameters");
                    }
                    return true;

                case "balance":
                    if (command.length == 2) {
                        try {
                            int accountId = Integer.parseInt(command[1]);

                            showBalance(accountId, dataSource);

                        } catch (NumberFormatException ex) {
                            ex.printStackTrace();
                        }
                    } else {
                        out.println("not enough parameters");
                    }
                    return true;

                case "deactivate":
                    if (command.length == 2) {
                        int accountId = Integer.parseInt(command[1]);
                        client.submit(new DeactivateAccountTxnContext(accountId, dataSource));
                    }
                    return true;

                default:
                    // Ignore;
                    return true;
            }
        } else {
            return true;
        }
    }

    private void showBalance(int accountId, DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT BALANCE, ACTIVE FROM ACCOUNTS WHERE ACCOUNT_ID = ?")) {
                stmt.setInt(1, accountId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int amount = rs.getInt(1);
                        String active = rs.getString(2);
                        out.println("\tBalance [accountId=" + accountId + " active=" + active + " amount=" + amount + "]");
                    } else {
                        out.println("\tBalance [accountId=" + accountId + " amount=NO_DATA]");
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    private class DemoBankAppClientCallbacks extends AbstractClientCallbacksForJDBC {

        DemoBankAppClientCallbacks(DataSource dataSource) {
            super(dataSource, CLIENT_HIGH_WATER_MARK_TABLE_NAME);
        }

        @Override
        protected void applyTransaction(Transaction transaction, Connection conn) throws SQLException {
            Map<String, String> data = transaction.getTransactionData(DemoSerializer.INSTANCE);

            String action = data.get("action");
            int accountId = Integer.parseInt(data.get("accountId"));
            int amount;

            switch (action) {
                case "deposit":
                    amount = Integer.parseInt(data.get("amount"));
                    updateBalance(accountId, amount, conn);
                    break;
                case "withdrawal":
                    amount = Integer.parseInt(data.get("amount"));
                    updateBalance(accountId, amount * -1, conn);
                    break;
                case "deactivate":
                    deactivateAccount(accountId, conn);
                    break;
                default:
                    // Ignore
            }
        }

        @Override
        public void uncaughtException(int partitionId, long transactionId, Throwable exception) {
            exception.printStackTrace();
        }

    }

    private void updateBalance(int accountId, int delta, Connection conn) throws SQLException {
        boolean updated;
        try (PreparedStatement stmt = conn.prepareStatement("UPDATE ACCOUNTS SET BALANCE = BALANCE + ? WHERE ACCOUNT_ID = ?")) {
            stmt.setInt(1, delta);
            stmt.setInt(2, accountId);
            stmt.execute();
            updated = stmt.getUpdateCount() > 0;
        }
        if (!updated) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO ACCOUNTS(ACCOUNT_ID, BALANCE, ACTIVE) VALUES(?, ?, 'Y')")) {
                stmt.setInt(1, accountId);
                stmt.setInt(2, delta);
                stmt.execute();
            }
        }
    }

    private void deactivateAccount(int accountId, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("UPDATE ACCOUNTS SET ACTIVE = 'N' WHERE ACCOUNT_ID = ?")) {
            stmt.setInt(1, accountId);
            stmt.execute();
        }
    }

    private class DepositTxnContext extends TransactionContext {

        final int accountId;
        final int amount;

        DepositTxnContext(int accountId, int amount) {
            this.accountId = accountId;
            this.amount = amount;
        }

        @Override
        public int partitionId(int numPartitions) {
            return accountId % numPartitions;
        }

        @Override
        public boolean execute(TransactionBuilder builder) {
            try (Connection conn = dataSource.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT ACTIVE FROM ACCOUNTS WHERE ACCOUNT_ID = ?")) {
                    stmt.setInt(1, accountId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            String active = rs.getString(1);
                            if (active.equals("N")) {
                                out.println("\tThe account is not active. [accountId=" + accountId + "]");
                                return false;
                            }
                        }
                    }

                    HashMap<String, String> data = new HashMap<>();
                    data.put("action", "deposit");
                    data.put("accountId", Integer.toString(accountId));
                    data.put("amount", Integer.toString(amount));

                    builder.setTransactionData(data, DemoSerializer.INSTANCE);

                    return true;
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
                return false;
            }
        }

        @Override
        public void onCompletion(boolean result) {
            if (result) {
                out.println("\tDeposit completed [accountId=" + accountId + " amount=" + amount + "]");
            } else {
                out.println("\tDeposit failed [accountId=" + accountId + " amount=" + amount + "]");
            }
        }

    }

    private class WithdrawalTxnContext extends TransactionContext {

        final int accountId;
        final int amount;
        final DataSource dataSource;

        WithdrawalTxnContext(int accountId, int amount, DataSource dataSource) {
            this.accountId = accountId;
            this.amount = amount;
            this.dataSource = dataSource;
        }

        @Override
        public int partitionId(int numPartitions) {
            return accountId % numPartitions;
        }

        @Override
        public boolean execute(TransactionBuilder builder) {
            try (Connection conn = dataSource.getConnection()) {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT BALANCE, ACTIVE FROM ACCOUNTS WHERE ACCOUNT_ID = ?")) {
                    stmt.setInt(1, accountId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            int balance = rs.getInt(1);
                            String active = rs.getString(2);
                            if (balance < amount || active.equals("N")) {
                                out.println("\tUnable to withdraw. [accountId=" + accountId + " active=" + active + " amount=" + amount + "]");
                                // There is not enough amount, or the account has been deactivated
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
                return false;
            }

            HashMap<String, String> data = new HashMap<>();

            data.put("action", "withdrawal");
            data.put("accountId", Integer.toString(accountId));
            data.put("amount", Integer.toString(amount));

            builder.setTransactionData(data, DemoSerializer.INSTANCE);
            builder.setWriteLocks(Collections.singletonList(new WithdrawalLock(accountId)));
            builder.setReadLocks(Collections.singletonList(new AccountLock(accountId)));

            return true;
        }

        @Override
        public void onCompletion(boolean result) {
            if (result) {
                out.println("\tWithdrawal completed [accountId=" + accountId + " amount=" + amount + "]");
            } else {
                out.println("\tWithdrawal failed [accountId=" + accountId + " amount=" + amount + "]");
            }
        }

    }

    private class DeactivateAccountTxnContext extends TransactionContext {

        final int accountId;
        final DataSource dataSource;

        DeactivateAccountTxnContext(int accountId, DataSource dataSource) {
            this.accountId = accountId;
            this.dataSource = dataSource;
        }

        @Override
        public int partitionId(int numPartitions) {
            return accountId % numPartitions;
        }

        @Override
        public boolean execute(TransactionBuilder builder) {
            HashMap<String, String> data = new HashMap<>();

            data.put("action", "deactivate");
            data.put("accountId", Integer.toString(accountId));

            builder.setTransactionData(data, DemoSerializer.INSTANCE);
            builder.setWriteLocks(Collections.singletonList(new AccountLock(accountId)));

            return true;
        }

        @Override
        public void onCompletion(boolean result) {
            if (result) {
                out.println("\tAccount deactivated [accountId=" + accountId + "]");
            } else {
                out.println("\tAccount deactivate failed [accountId=" + accountId + "]");
            }
        }

    }

    private static class WithdrawalLock extends PartitionLocalLock {

        WithdrawalLock(int accountId) {
            super("withdrawal", (long) accountId);
        }

    }

    private static class AccountLock extends PartitionLocalLock {

        AccountLock(int accountId) {
            super("account", (long) accountId);
        }

    }

    private static class DemoBankAppCli extends Cli {
        private String configPath;

        DemoBankAppCli(String[] args) {
            super(args);
        }

        @Override
        protected void processCmd(CommandLine cmd) {
            int argLength = cmd.getArgList().size();
            if (argLength > 0) {
                configPath = cmd.getArgList().get(argLength - 1);
            }
        }

        @Override
        protected void configureOptions(Options options) { }

        @Override
        protected String getUsage() {
            return "DemoBankApp <config_path>";
        }

        String getConfigPath() {
            return configPath;
        }
    }

    private static class DemoBankAppConfig extends AbstractConfig {
        static final String DS_CONNECT_STRING = "DataSource.ConnectString";
        static final String DS_USERNAME = "DataSource.Username";
        static final String DS_PASSWORD = "DataSource.Password";

        // ZooKeeper
        static final String ZOOKEEPER_CONNECT_STRING = "zookeeper.connectString";
        static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.sessionTimeout";

        // Cluster
        static final String CLUSTER_ROOT = "cluster.root";

        private static final HashMap<String, Parser> parsers = new HashMap<>();
        static {
            parsers.put(DS_CONNECT_STRING, stringParser);
            parsers.put(DS_USERNAME, stringParser);
            parsers.put(DS_PASSWORD, stringParser);

            // ZooKeeper
            parsers.put(ZOOKEEPER_CONNECT_STRING, stringParser);
            parsers.put(ZOOKEEPER_SESSION_TIMEOUT, intParser);

            // Cluster
            parsers.put(CLUSTER_ROOT, stringParser);
        }

        DemoBankAppConfig(Map<Object, Object> configValues) {
            this("", configValues);
        }

        DemoBankAppConfig(String configPrefix, Map<Object, Object> configValues) {
            super(configPrefix, configValues, parsers);
        }
    }
}
