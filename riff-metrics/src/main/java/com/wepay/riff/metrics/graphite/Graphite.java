package com.wepay.riff.metrics.graphite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

/**
 * A client to a Graphite server using TCP.
 */
public class Graphite implements GraphiteSender {

    private static final Logger LOG = LoggerFactory.getLogger(Graphite.class);

    private InetSocketAddress address;
    private SocketFactory socketFactory;
    private Socket socket;
    private Writer writer;
    private Charset charset;

    /**
     * Creates a new client which connects to the given hostname and port using the default
     * {@link SocketFactory}.
     * @param hostname The hostname of the server.
     * @param port The port of the server.
     */
    public Graphite(final String hostname, final int port) {
        this(hostname, port, Charset.forName("UTF-8"));
    }

    /**
     * Creates a new client which connects to the given hostname and port using the default
     * {@link SocketFactory}.
     * @param hostname The hostname of the server.
     * @param port The port of the server.
     * @param charset The {@link Charset} to use.
     */
    public Graphite(final String hostname, final int port, final Charset charset) {
        this(new InetSocketAddress(hostname, port), SocketFactory.getDefault(), charset);
    }

    /**
     * Creates a new client which connects to the given address using the provided
     * {@link SocketFactory}.
     * @param address The address of the server.
     * @param socketFactory The {@link SocketFactory} to use.
     * @param charset The {@link Charset} to use.
     */
    private Graphite(final InetSocketAddress address, final SocketFactory socketFactory, final Charset charset) {
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
    public void connect() throws IOException {
        if (isConnected()) {
            throw new IllegalStateException("Already connected");
        }
        if (address.getHostName() != null) {
            // retry lookup, just in case the DNS changed
            address = new InetSocketAddress(address.getHostName(), address.getPort());

            if (address.getAddress() == null) {
                throw new UnknownHostException(address.getHostName());
            }
        }

        this.socket = socketFactory.createSocket(address.getAddress(), address.getPort());
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), this.charset));
    }

    /**
     * Sends the given measurement to the server.
     * @param name the name of the metric.
     * @param value The value of the metric.
     * @param timestamp The timestamp of the metric.
     */
    public void send(final String name, final String value, long timestamp) throws IOException {
        try {
            writer.write(sanitize(name));
            writer.write(' ');
            writer.write(sanitize(value));
            writer.write(' ');
            writer.write(Long.toString(timestamp));
            writer.write('\n');
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * Flushes the writer.
     */
    public void flush() throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }

    /**
     * Close the socket as well as the writer.
     */
    @Override
    public void close() throws IOException {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ex) {
            LOG.debug("Error closing writer", ex);
        } finally {
            this.writer = null;
        }

        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ex) {
            LOG.debug("Error closing socket", ex);
        } finally {
            this.socket = null;
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }
}
