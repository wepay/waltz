package com.wepay.riff.metrics.graphite;

import java.io.Closeable;
import java.io.IOException;
import java.util.regex.Pattern;

public interface GraphiteSender extends Closeable {

    /**
     * Connects to the server.
     *
     * @throws IllegalStateException if the client is already connected
     * @throws IOException           if there is an error connecting
     */
    void connect() throws IllegalStateException, IOException;

    /**
     * Sends the given measurement to the server.
     *
     * @param name      the name of the metric
     * @param value     the value of the metric
     * @param timestamp the timestamp of the metric
     * @throws IOException if there was an error sending the metric
     */
    void send(String name, String value, long timestamp) throws IOException;

    /**
     * Flushes buffer, if applicable.
     *
     * @throws IOException if there was an error during flushing metrics to the socket.
     */
    void flush() throws IOException;

    /**
     * Returns true if ready to send data.
     * @return returns true if ready to send data
     */
    boolean isConnected();

    default String sanitize(String input) {
        return Pattern.compile("[\\s]+").matcher(input.trim()).replaceAll("-");
    }
}
