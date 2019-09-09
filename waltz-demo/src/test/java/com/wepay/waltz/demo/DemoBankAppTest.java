package com.wepay.waltz.demo;

import com.wepay.waltz.client.WaltzClientConfig;
import com.wepay.zktools.util.Uninterruptibly;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;

public class DemoBankAppTest {

    private DemoServers demoServers;
    private DataSource dataSource;
    private Connection dummyConnection; // keep this connection open during the test to pin the tables in memory

    @Before
    public void setup() throws Exception {
        demoServers = new DemoServers();
        demoServers.startServers();
        dataSource = new TestDataSource();
        dummyConnection = dataSource.getConnection();
    }

    @After
    public void teardown() throws Exception {
        dummyConnection.close();
        dummyConnection = null;
        dataSource = null;
        demoServers.shutdownServers();
        demoServers = null;
    }

    @Test
    public void test() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos, true, "UTF-8");
        DemoBankApp demoBankApp = new DemoBankApp(dataSource, config(), out);

        demoBankApp.initWaltzClient();

        demoBankApp.processCmd("deposit", "1", "100");
        demoBankApp.processCmd("deposit", "2", "200");
        Uninterruptibly.sleep(500);

        baos.reset();
        demoBankApp.processCmd("balance", "1");
        assertTrue(baos.toString("UTF-8").contains("Balance [accountId=1 active=Y amount=100]"));

        baos.reset();
        demoBankApp.processCmd("balance", "2");
        assertTrue(baos.toString("UTF-8").contains("Balance [accountId=2 active=Y amount=200]"));

        demoBankApp.processCmd("withdraw", "1", "100");
        demoBankApp.processCmd("withdraw", "2", "100");
        Uninterruptibly.sleep(500);

        baos.reset();
        demoBankApp.processCmd("balance", "1");
        assertTrue(baos.toString("UTF-8").contains("Balance [accountId=1 active=Y amount=0]"));

        baos.reset();
        demoBankApp.processCmd("balance", "2");
        assertTrue(baos.toString("UTF-8").contains("Balance [accountId=2 active=Y amount=100]"));

        demoBankApp.processCmd("deactivate", "2");
        Uninterruptibly.sleep(500);

        baos.reset();
        demoBankApp.processCmd("balance", "1");
        assertTrue(baos.toString("UTF-8").contains("Balance [accountId=1 active=Y amount=0]"));
        baos.reset();
        demoBankApp.processCmd("balance", "2");
        assertTrue(baos.toString("UTF-8").contains("Balance [accountId=2 active=N amount=100]"));
    }

    private WaltzClientConfig config() {
        Properties props = new Properties();
        props.setProperty(WaltzClientConfig.ZOOKEEPER_CONNECT_STRING, demoServers.zkConnectString());
        props.setProperty(WaltzClientConfig.ZOOKEEPER_SESSION_TIMEOUT, Integer.toString(DemoServers.ZK_SESSION_TIMEOUT));
        props.setProperty(WaltzClientConfig.CLUSTER_ROOT, DemoServers.CLUSTER_ROOT_ZNODE.toString());
        return new WaltzClientConfig(props);
    }

    private static class TestDataSource implements DataSource {
        private static final String CONNECT_STRING = "jdbc:h2:mem:DemoBank;MODE=MYSQL";

        @Override
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(CONNECT_STRING);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new UnsupportedOperationException();
        }

    }

}
