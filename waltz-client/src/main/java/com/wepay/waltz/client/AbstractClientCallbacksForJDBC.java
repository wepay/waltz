package com.wepay.waltz.client;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.util.BackoffTimer;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A sample implementation of client callbacks for applications with a shared database.
 * The client database must have the following table. (&lt;clientHighWaterMarkTableName&gt; is specified in the constructor parameter.)
 * <pre>
 *     CREATE TABLE &lt;clientHighWaterMarkTableName&gt; (
 *         PARTITION_ID INTEGER NOT NULL,
 *         HIGH_WATER_MARK BIGINT NOT NULL,
 *
 *         PRIMARY KEY (PARTITION_ID)
 *     );
 * </pre>
 */
public abstract class AbstractClientCallbacksForJDBC implements WaltzClientCallbacks {

    private static final Logger logger = Logging.getLogger(AbstractClientCallbacksForJDBC.class);

    private static final long INITIAL_RETRY_INTERVAL = 10;
    private static final long MAX_RETRY_INTERVAL = 10000;

    // Selects the specified partition's high-water mark.
    private static final String SELECT_HIGH_WATER_MARK =
        "SELECT HIGH_WATER_MARK FROM %s WHERE PARTITION_ID = ?";

    // Updates the specified partition's high-water mark only when we are at the expected high-water mark
    private static final String UPDATE_HIGH_WATER_MARK =
        "UPDATE %s SET HIGH_WATER_MARK = HIGH_WATER_MARK + 1 "
            + "WHERE PARTITION_ID = ? AND HIGH_WATER_MARK = ?";

    // Insert the specified partition's initial high-water mark (-1L) to the table
    private static final String INSERT_HIGH_WATER_MARK =
        "INSERT INTO %s (PARTITION_ID, HIGH_WATER_MARK) VALUES (?, -1)";

    private final DataSource dataSource;

    private final String selectHighWaterMark;
    private final String updateHighWaterMark;
    private final String insertHighWaterMark;

    private final ConcurrentHashMap<Integer, HighWaterMark> highWaterMarkCache = new ConcurrentHashMap<>();
    private final BackoffTimer backoffTimer = new BackoffTimer(MAX_RETRY_INTERVAL);

    /**
     * Class Constructor.
     *
     * @param dataSource the {@link DataSource} to communicate with the underlying Sql database.
     * @param clientHighWaterMarkTableName the client high-water mark table name.
     */
    public AbstractClientCallbacksForJDBC(DataSource dataSource, String clientHighWaterMarkTableName) {
        this.dataSource = dataSource;

        this.selectHighWaterMark = String.format(SELECT_HIGH_WATER_MARK, clientHighWaterMarkTableName);
        this.updateHighWaterMark = String.format(UPDATE_HIGH_WATER_MARK, clientHighWaterMarkTableName);
        this.insertHighWaterMark = String.format(INSERT_HIGH_WATER_MARK, clientHighWaterMarkTableName);
    }

    /**
     * A subclass must override this method to implement a logic that applies the transaction to the application's database.
     * For the transaction consistency, the method must use the connection passed in to update the database and
     * must not commit the SQL transaction.
     *
     * @param transaction the transaction
     * @param connection JDBC connection
     * @throws SQLException Thrown if there's an issue fetching high watermark
     */
    protected abstract void applyTransaction(Transaction transaction, Connection connection) throws SQLException;

    /**
     * For a given partition, gets the high-water mark from the client high-water mark table and updates the {@link #highWaterMarkCache} cache.
     *
     * Retries indefinitely, with a backOff interval of {@link #INITIAL_RETRY_INTERVAL} millis, in case of exceptions.
     *
     * @param partitionId the partition id to get client high-water mark for.
     * @return the high-water mark.
     */
    @Override
    public long getClientHighWaterMark(int partitionId) {
        long retryInterval = INITIAL_RETRY_INTERVAL;
        int attempts = 0;

        while (true) {
            try {
                Connection connection = dataSource.getConnection();
                try {
                    connection.setAutoCommit(true);

                    // Read the high-water mark from database and update the cache .
                    return selectHighWaterMark(partitionId, connection);

                } finally {
                    closeSafely(connection);
                }
            } catch (SQLException ex) {
                onErrorGettingClientHighWaterMark(++attempts, ex);

                // Retry
                retryInterval = backoffTimer.backoff(retryInterval);
            }
        }
    }

    /**
     * Handles SQLException raised in {@link #getClientHighWaterMark(int)}.
     *
     * @param attempts the number of trials to get the client high-water mark.
     * @param exception the SQLException raised in {@link #getClientHighWaterMark(int)}.
     */
    protected void onErrorGettingClientHighWaterMark(int attempts, SQLException exception) {
        // A subclass may override this.
    }

    /**
     * If <pre>{@code transactionId - 1 == highWaterMark}</pre>;
     * in a database transaction with {@link Connection#TRANSACTION_SERIALIZABLE} isolation and autoCommit as false:
     *      1. Calls {@link #applyTransaction(Transaction, Connection)} which should be implemented by sub-class.
     *      2. Updates the high-water mark.
     *
     * Tries indefinitely as long as the above condition holds.
     *
     * @param transaction the committed transaction.
     */
    @Override
    public void applyTransaction(Transaction transaction) {
        final int partitionId = transaction.reqId.partitionId();

        final HighWaterMark highWaterMark = highWaterMarkCache.get(partitionId);
        final long transactionId = transaction.transactionId;

        long retryInterval = INITIAL_RETRY_INTERVAL;

        synchronized (highWaterMark) {
            while (transactionId - 1 == highWaterMark.get()) {
                try {
                    Connection connection = dataSource.getConnection();
                    try {
                        connection.setAutoCommit(false);
                        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

                        PreparedStatement stmt = connection.prepareStatement(updateHighWaterMark);
                        try {
                            stmt.setInt(1, partitionId);
                            stmt.setLong(2, transactionId - 1L);
                            stmt.execute();

                            if (stmt.getUpdateCount() == 1) {
                                // Successfully updated the partition's high-water mark. We are holding the write lock.
                                // Apply the transaction.
                                applyTransaction(transaction, wrapConnection(connection));
                                connection.commit();

                                // Update the high-water mark cache
                                highWaterMark.trySet(transactionId);

                            } else {
                                // Skip this transaction (already applied by some other process)
                                // Read the high-water mark from database and update the cache.
                                selectHighWaterMark(partitionId, connection);
                                connection.commit();
                            }

                            return;

                        } finally {
                            closeSafely(stmt);
                        }

                    } catch (Throwable ex) {
                        try {
                            connection.rollback();
                        } catch (SQLException rollbackerror) {
                            logger.debug("failed to rollback", rollbackerror);
                        }
                        throw ex;

                    } finally {
                        closeSafely(connection);
                    }

                } catch (SQLException ex) {
                    // Retry
                    retryInterval = backoffTimer.backoff(retryInterval);
                }
            }
        }
    }

    private void insert(int partitionId, Connection connection) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(insertHighWaterMark);
        try {
            stmt.setInt(1, partitionId);
            stmt.execute();

        } finally {
            closeSafely(stmt);
        }
    }

    private long selectHighWaterMark(int partitionId, Connection connection) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(selectHighWaterMark);
        try {
            stmt.setInt(1, partitionId);

            while (true) {
                ResultSet rs = stmt.executeQuery();
                try {
                    if (rs.next()) {
                        long highWaterMark = rs.getLong(1);

                        return updateHighWaterMarkCache(partitionId, highWaterMark);

                    } else {
                        // The row not found, insert a new one
                        try {
                            insert(partitionId, connection);
                        } catch (SQLException ex) {
                            // Ignore (This may be a benign race condition.)
                        }
                    }
                } finally {
                    closeSafely(rs);
                }
            }

        } finally {
            closeSafely(stmt);
        }
    }

    private long updateHighWaterMarkCache(int partitionId, long highWaterMark) {
        HighWaterMark cached = highWaterMarkCache.get(partitionId);
        if (cached == null) {
            cached = highWaterMarkCache.putIfAbsent(partitionId, new HighWaterMark(highWaterMark));
        }

        if (cached == null) {
            return highWaterMark;
        } else {
            synchronized (cached) {
                return cached.trySet(highWaterMark);
            }
        }
    }

    private static class HighWaterMark {
        private long value;

        HighWaterMark(long value) {
            this.value = value;
        }

        long trySet(long value) {
            // Ensure monotonic increase
            if (this.value < value) {
                this.value = value;
            }
            return this.value;
        }

        long get() {
            return value;
        }
    }

    private Connection wrapConnection(final Connection connection) {
        return (Connection) Proxy.newProxyInstance(this.getClass().getClassLoader(),
            new Class[] {
                Connection.class
            },
            new InvocationHandler() {
                public Object invoke(Object proxy, Method method,
                                     Object[] args) throws Throwable {
                    switch (method.getName()) {
                        case "commit":
                            throw new SQLException("commit is forbidden");
                        case "rollback":
                            throw new SQLException("rollback is forbidden");
                        default:
                            return method.invoke(connection, args);
                    }
                }
            }
        );
    }

    private void closeSafely(PreparedStatement stmt) {
        try {
            stmt.close();
        } catch (SQLException ex) {
            // Ignore
        }
    }

    private void closeSafely(ResultSet rs) {
        try {
            rs.close();
        } catch (SQLException ex) {
            // Ignore
        }
    }

    private void closeSafely(Connection connection) {
        try {
            connection.close();
        } catch (SQLException ex) {
            // Ignore
        }
    }

}
