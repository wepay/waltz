package com.wepay.waltz.demo;

import com.wepay.waltz.client.WaltzClient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class DemoAppBase implements Runnable, Closeable {

    protected final DataSource dataSource;

    protected WaltzClient client = null;

    private final BufferedReader reader;

    protected DemoAppBase(DataSource dataSource) {
        this.dataSource = dataSource;
        this.reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException ex) {
            // Ignore
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    protected String[] prompt(String str) throws IOException {
        System.out.print(str);
        String line = reader.readLine();

        if (line != null) {
            return line.trim().split("\\s+");
        } else {
            return null;
        }
    }

    protected static void executeSQL(String sql, Connection connection, boolean ignoreException) throws SQLException {
        try {
            connection.setAutoCommit(true);
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.execute();
            }

        } catch (SQLException ex) {
            if (!ignoreException) {
                throw ex;
            } else {
                new Exception("Failed to execute: " + sql, ex).printStackTrace();
            }
        }
    }

    @SuppressFBWarnings(value = "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING", justification = "internal use")
    protected void createClientHighWaterMarkTable(Connection connection, String clientHighWaterMarkTableName) throws SQLException {
        executeSQL(
            "DROP TABLE IF EXISTS " + clientHighWaterMarkTableName,
            connection,
            true // ignore exception
        );
        executeSQL(
            "CREATE TABLE " + clientHighWaterMarkTableName + " ("
                + "PARTITION_ID INTEGER NOT NULL,"
                + "HIGH_WATER_MARK BIGINT NOT NULL,"
                + "PRIMARY KEY (PARTITION_ID)"
                + ")",
            connection,
            false
        );
    }

}
