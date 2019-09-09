package com.wepay.riff.util;

import com.wepay.zktools.util.Uninterruptibly;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public final class PortFinder {

    private static final int DEFAULT_PORT_SEARCH_START = 10000;
    private static final int DEFAULT_PORT_SEARCH_END = 12000;

    private final int from;
    private final int to;
    private final Iterator<Integer> iterator;

    public PortFinder() {
        this(DEFAULT_PORT_SEARCH_START, DEFAULT_PORT_SEARCH_END);
    }

    public PortFinder(int from, int to) {
        this.from = from;
        this.to = to;

        ArrayList<Integer> ports = new ArrayList<>();

        for (int port = from; port < to + 1; port++) {
            ports.add(port);
        }
        Collections.shuffle(ports);

        this.iterator = ports.iterator();
    }

    public int getPort() {
        synchronized (this) {

            while (iterator.hasNext()) {
                Integer port = iterator.next();
                try {
                    new ServerSocket(port, 0, InetAddress.getLocalHost()).close();
                    Uninterruptibly.sleep(100);

                    return port;

                } catch (UnknownHostException e) {
                    throw new RuntimeException("unable to get host name", e);

                } catch (IOException e) {
                    // In use. Ignore
                }
            }
        }
        throw new RuntimeException("unable to find a free port in range [" + from + ", " + to + "]");
    }

}
