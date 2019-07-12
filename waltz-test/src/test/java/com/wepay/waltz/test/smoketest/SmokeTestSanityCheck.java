package com.wepay.waltz.test.smoketest;

import org.junit.Test;

import static org.junit.Assert.fail;

public class SmokeTestSanityCheck {

    @Test
    public void test() throws Exception {
        SmokeTest test = null;
        try {
            // one transaction, one client, one partition, no lock
            test = new SmokeTest(1, 1, 1, 0);
            test.open();

            test.createSchedulers();
            test.startWaltzStorages();
            test.startWaltzServers();

            test.test();

            test.shutdownWaltzServers();
            test.shutdownWaltzStorages();

        } catch (Exception ex) {
            fail(ex.getMessage());

        } finally {
            if (test != null) {
                test.close();
            }
        }
    }

}
