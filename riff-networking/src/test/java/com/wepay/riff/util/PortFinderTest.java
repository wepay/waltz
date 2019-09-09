package com.wepay.riff.util;

import org.junit.Test;

import java.net.InetAddress;
import java.net.ServerSocket;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PortFinderTest {

    @Test
    public void test() throws Exception {
        int port = new PortFinder().getPort();

        try (ServerSocket serverSocket = new ServerSocket(port, 50, InetAddress.getLocalHost())) {
            try {
                PortFinder portFinder = new PortFinder(port, port);
                portFinder.getPort();
                fail();

            } catch (RuntimeException ex) {
                assertTrue(ex.toString().contains("unable to find a free port in range"));
            }
        }
    }

}
