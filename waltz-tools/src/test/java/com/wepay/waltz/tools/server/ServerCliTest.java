package com.wepay.waltz.tools.server;

import com.wepay.waltz.test.util.IntegrationTestHelper;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import static junit.framework.TestCase.assertTrue;

public final class ServerCliTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    @Before
    public void setUpStreams() throws UnsupportedEncodingException {
        System.setOut(new PrintStream(outContent, false, "UTF-8"));
        System.setErr(new PrintStream(errContent, false, "UTF-8"));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testListPartition() throws Exception {
        Properties properties =  new Properties();
        properties.setProperty(IntegrationTestHelper.Config.ZNODE_PATH, "/server/cli/test");
        properties.setProperty(IntegrationTestHelper.Config.NUM_PARTITIONS, "3");
        properties.setProperty(IntegrationTestHelper.Config.ZK_SESSION_TIMEOUT, "30000");

        String metricGroup = "waltz-server-" + UUID.randomUUID().toString();
        properties.setProperty(IntegrationTestHelper.Config.METRIC_GROUP, metricGroup);

        IntegrationTestHelper helper = new IntegrationTestHelper(properties);
        int jettyPort = helper.getServerJettyPort();

        try {
            helper.startZooKeeperServer();
            helper.startWaltzServer(true);

            String[] args = {
                    "list",
                    "--server",
                    helper.getHost() + ":" + jettyPort,
                    "--metric_group",
                    metricGroup
            };
            ServerCli.testMain(args);
            String expectedCmdOutput = "There are " + helper.getNumPartitions() + " partitions for current server";
            assertTrue(outContent.toString("UTF-8").contains(expectedCmdOutput));
        } finally {
            helper.closeAll();
        }
    }

}
