package com.wepay.waltz.common.metadata.store.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ConnectionMetadata contains storage node connection metadata in Zookeeper. With current
 * data structure, each connect string is mapped to an admin port. It can be persist to
 * and consume from Zookeeper with {@link ConnectionMetadataSerializer}.
 */
public class ConnectionMetadata {

    private static final byte VERSION = 1;

    public final Map<String, Integer> connections; // <connect_string, admin_port>

    /**
     * Initialize ConnectionMetadata with a copy of connections.
     * @param connections a map of (storage_node_connect_string, admin_port).
     */
    public ConnectionMetadata(final Map<String, Integer> connections) {
        this(connections, true);
    }

    /**
     * Initialize ConnectionMetadata with connections or a copy of connections.
     * @param connections a map of <storage_node_connect_string, admin_port>.
     * @param copy Boolean flag indicating whether to create a copy or not.
     */
    private ConnectionMetadata(final Map<String, Integer> connections, boolean copy) {
        this.connections = copy ? new HashMap<>(connections) : connections;
    }

    /**
     * Writes storage node connection metadata via the {@link DataOutput} provided.
     * @param out The interface that converts the data to a series of bytes.
     * @throws IOException thrown if the write fails.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeInt(connections.size());
        for (Map.Entry<String, Integer> entry : connections.entrySet()) {
            // the storage node connect string
            out.writeUTF(entry.getKey());
            // the storage node admin port
            out.writeInt(entry.getValue());
        }
    }

    /**
     * Reads storage node connection metadata via the {@link DataInput} provided.
     * @param in The interface that reads bytes from a binary stream and converts it
     *        to the data of required type.
     * @return Returns the storage node's {@code ConnectionMetadata}.
     * @throws IOException thrown if the read fails.
     */
    public static ConnectionMetadata readFrom(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException("unsupported version");
        }

        int size = in.readInt();
        Map<String, Integer> connections = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String connectString = in.readUTF();
            int adminPort = in.readInt();
            connections.put(connectString, adminPort);
        }

        return new ConnectionMetadata(connections, false);
    }

    @Override
    public int hashCode() {
        return connections.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ConnectionMetadata) {
            ConnectionMetadata other = (ConnectionMetadata) o;

            if (!this.connections.keySet().equals(other.connections.keySet())) {
                return false;
            }
            for (Map.Entry<String, Integer> entry : this.connections.entrySet()) {
                if (entry.getValue().intValue() != other.connections.get(entry.getKey()).intValue()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
}
