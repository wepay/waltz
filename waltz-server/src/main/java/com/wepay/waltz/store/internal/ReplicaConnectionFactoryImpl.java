package com.wepay.waltz.store.internal;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.storage.client.StorageClient;
import com.wepay.waltz.storage.exception.StorageRpcException;
import com.wepay.waltz.store.exception.ReplicaConnectionFactoryClosedException;
import org.slf4j.Logger;

/**
 * Implements {@link ReplicaConnection}.
 */
public class ReplicaConnectionFactoryImpl implements ReplicaConnectionFactory {

    private static final Logger logger = Logging.getLogger(ReplicaConnectionFactoryImpl.class);

    public final String connectString;

    private final String host;
    private final int port;
    private final ConnectionConfig config;

    private volatile StorageClient client = null;
    private volatile boolean running = true;

    /**
     * Class constructor.
     * @param connectString Storage node connection string (in host:port format).
     * @param config The connection config.
     */
    public ReplicaConnectionFactoryImpl(final String connectString, ConnectionConfig config) {
        this.connectString = connectString;
        this.config = config;

        try {
            String[] components = connectString.split(":");

            this.host = components[0];
            this.port = Integer.parseInt(components[1]);

        } catch (Exception ex) {
            logger.error("malformed connect string: "  + connectString, ex);
            throw new IllegalArgumentException("malformed connect string: "  + connectString, ex);
        }
    }

    /**
     * Returns the {@link ReplicaConnection}.
     * @param partitionId The partition Id.
     * @param sessionId The session Id.
     * @return {@code ReplicaConnection}.
     * @throws StorageRpcException thrown if Storage connection fails.
     * @throws ReplicaConnectionFactoryClosedException thrown if the replica connection is closed.
     */
    public ReplicaConnection get(int partitionId, long sessionId) throws ReplicaConnectionFactoryClosedException, StorageRpcException {
        return new ReplicaConnectionImpl(partitionId, sessionId, getStorageClient());
    }

    /**
     * Closes the replica connection factory.
     */
    public void close() {
        synchronized (this) {
            running = false;
            if (client != null) {
                if (!client.isDisconnected()) {
                    client.close();
                }
                client = null;
            }
        }
    }

    private StorageClient getStorageClient() throws ReplicaConnectionFactoryClosedException {
        synchronized (this) {
            if (running) {
                if (client != null && !client.isValid()) {
                    client.close();
                    client = null;
                }

                if (client == null) {
                    client = new StorageClient(host, port, config.sslCtx, config.key, config.numPartitions);
                    try {
                        client.open();
                    } catch (Exception e) {
                        client.close();
                        client = null;
                        throw e;
                    }
                }
                return client;
            } else {
                throw new ReplicaConnectionFactoryClosedException();
            }
        }
    }

}
