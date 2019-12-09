package com.wepay.waltz.client;

import com.wepay.riff.util.Logging;
import com.wepay.waltz.common.util.BackoffTimer;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

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
                                applyTransaction(transaction, new ConnectionWrapper(connection));
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

    public static class ForbiddenJdbcCallException extends SQLException {
        public final String methodName;

        ForbiddenJdbcCallException(String methodName) {
            super(methodName + " is forbidden");

            this.methodName = methodName;
        }
    }

    private static class ConnectionWrapper implements Connection {

        private final Connection connection;

        ConnectionWrapper(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return connection.createStatement();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return connection.prepareStatement(sql);
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return connection.prepareCall(sql);
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return connection.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            throw new ForbiddenJdbcCallException("setAutoCommit");
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return connection.getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            throw new ForbiddenJdbcCallException("commit");
        }

        @Override
        public void rollback() throws SQLException {
            throw new ForbiddenJdbcCallException("rollback");
        }

        @Override
        public void close() throws SQLException {
            connection.close();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return connection.isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return connection.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            throw new ForbiddenJdbcCallException("setReadOnly");
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return connection.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            throw new ForbiddenJdbcCallException("setCatalog");
        }

        @Override
        public String getCatalog() throws SQLException {
            return connection.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            throw new ForbiddenJdbcCallException("setTransactionIsolation");
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return connection.getTransactionIsolation();
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return connection.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            connection.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return connection.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return connection.getTypeMap();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            connection.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            connection.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return connection.getHoldability();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return connection.setSavepoint();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return connection.setSavepoint(name);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            connection.rollback(savepoint);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            connection.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return connection.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return connection.prepareStatement(sql, columnIndexes);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return connection.prepareStatement(sql, columnNames);
        }

        @Override
        public Clob createClob() throws SQLException {
            return connection.createClob();
        }

        @Override
        public Blob createBlob() throws SQLException {
            return connection.createBlob();
        }

        @Override
        public NClob createNClob() throws SQLException {
            return connection.createNClob();
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return connection.createSQLXML();
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return connection.isValid(timeout);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            connection.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            connection.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return connection.getClientInfo(name);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return connection.getClientInfo();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return connection.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return connection.createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            connection.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            return connection.getSchema();
        }

        @Override
        public void abort(Executor executor) throws SQLException {
            connection.abort(executor);
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            connection.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return connection.getNetworkTimeout();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new ForbiddenJdbcCallException("unwrap");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return connection.isWrapperFor(iface);
        }
    }

}
