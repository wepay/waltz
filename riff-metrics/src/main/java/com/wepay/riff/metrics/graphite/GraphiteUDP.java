package com.wepay.riff.metrics.graphite;

import com.wepay.riff.metrics.common.DatagramSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

/**
 * A client to a Graphite server using UDP.
 */
public class GraphiteUDP implements GraphiteSender {

    private static final Logger LOG = LoggerFactory.getLogger(GraphiteUDP.class);

    private final DatagramSocketFactory socketFactory;
    private Charset charset;
    private InetSocketAddress address;
    private DatagramSocket socket;

    /**
     * Creates a new client that connects to the given hostname and port.
     * @param hostname The hostname of the server.
     * @param port The port of the server.
     */
    public GraphiteUDP(final String hostname, final int port) {
        this(hostname, port, Charset.forName("UTF-8"));
    }

    /**
     * Creates a new client which connects to the given hostname, port and charset.
     * @param hostname The hostname of the server.
     * @param port The port of the server.
     * @param charset The {@link Charset} to use.
     */
    public GraphiteUDP(final String hostname, final int port, final Charset charset) {
        this(new InetSocketAddress(hostname, port), new DatagramSocketFactory(), charset);
    }

    /**
     * Creates a new client that connects to the given address using the given {@link DatagramSocketFactory}
     * @param address The address of the server.
     * @param socketFactory The {@link DatagramSocketFactory} to use.
     * @param charset The {@link Charset} to use.
     */
    private GraphiteUDP(final InetSocketAddress address, final DatagramSocketFactory socketFactory, final Charset charset) {
        this.address = address;
        this.socketFactory = socketFactory;
        this.charset = charset;
    }

    /**
     * Connects to the server and creates a new socket.
     * Also resolves the hostname if not already done.
     * @throws IllegalStateException if the client is already connected.
     * @throws IOException if there is any error connecting to the server.
     */
    @Override
    public void connect() throws IllegalStateException, IOException {
        if (isConnected()) {
            throw new IllegalStateException("Already Connected");
        }
        if (address.getHostName() != null) {
            // retry lookup, just in case the DNS changed
            address = new InetSocketAddress(address.getHostName(), address.getPort());

            if (address.getAddress() == null) {
                throw new UnknownHostException(address.getHostName());
            }
        }
        this.socket = socketFactory.createSocket();
    }

    /**
     * Sends the given measurement to the server.
     * @param name the name of the metric.
     * @param value The value of the metric.
     * @param timestamp The timestamp of the metric.
     */
    @Override
    public void send(final String name, final String value, final long timestamp) throws IOException {
        try {
            String str = sanitize(name) + ' ' + sanitize(value) + ' ' + timestamp + '\n';
            byte[] bytes = str.getBytes(charset);
            socket.send(socketFactory.createPacket(bytes, bytes.length, address));
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    public void flush() throws IOException {
        // Nothing to be done here.
    }

    @Override
    public boolean isConnected() {
        return socket != null;
    }

    /**
     * Close the socket.
     */
    @Override
    public void close() throws IOException {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {
            LOG.debug("Error closing socket", e);
        } finally {
            socket = null;
        }
    }

}
