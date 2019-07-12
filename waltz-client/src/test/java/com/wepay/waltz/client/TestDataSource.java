package com.wepay.waltz.client;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class TestDataSource implements DataSource {

    private final AtomicInteger currentSeq = new AtomicInteger(0);
    private final AtomicInteger numConnectionsMade = new AtomicInteger(0);
    private final String connectString;

    public TestDataSource(String schema) {
        this.connectString = "jdbc:h2:mem:" + schema + ";MODE=MYSQL";
    }

    @Override
    public Connection getConnection() throws SQLException {
        numConnectionsMade.incrementAndGet();
        return wrapConnection(DriverManager.getConnection(connectString), currentSeq.get());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException();
    }

    public int numConnectionsMade() {
        return numConnectionsMade.get();
    }

    public void makeDbUp() {
        int currentSeq = this.currentSeq.get();

        while ((currentSeq & 1) == 1) {
            if (this.currentSeq.compareAndSet(currentSeq, currentSeq + 1)) {
                break;
            }
            currentSeq = this.currentSeq.get();
        }
    }

    public void makeDbDown() {
        int currentSeq = this.currentSeq.get();

        while ((currentSeq & 1) == 0) {
            if (this.currentSeq.compareAndSet(currentSeq, currentSeq + 1)) {
                break;
            }
            currentSeq = this.currentSeq.get();
        }
    }

    private Connection wrapConnection(final Connection connection, final int thisSeq) {
        return (Connection) Proxy.newProxyInstance(this.getClass().getClassLoader(),
            new Class[] {
                Connection.class
            },
            new InvocationHandler() {
                public Object invoke(Object proxy, Method method,
                                     Object[] args) throws Throwable {
                    if (thisSeq == currentSeq.get() && (thisSeq & 1) == 0) {
                        Object ret = method.invoke(connection, args);
                        if (ret instanceof PreparedStatement) {
                            return wrapPreparedStatement((PreparedStatement) ret, connection, thisSeq);
                        } else {
                            return ret;
                        }
                    } else {
                        connection.rollback();
                        if ("close".equals(method.getName())) {
                            return method.invoke(connection, args);
                        } else {
                            throw new SQLException("simulated disconnect");
                        }
                    }
                }
            }
        );
    }

    private PreparedStatement wrapPreparedStatement(final PreparedStatement stmt, final Connection connection, final int thisSeq) {
        return (PreparedStatement) Proxy.newProxyInstance(this.getClass().getClassLoader(),
            new Class[] {
                PreparedStatement.class
            },
            new InvocationHandler() {
                public Object invoke(Object proxy, Method method,
                                     Object[] args) throws Throwable {
                    if (thisSeq == currentSeq.get() && (thisSeq & 1) == 0) {
                        return method.invoke(stmt, args);
                    } else {
                        connection.rollback();
                        if ("close".equals(method.getName())) {
                            return method.invoke(stmt, args);
                        } else {
                            throw new SQLException("simulated disconnect");
                        }
                    }
                }
            }
        );
    }

}
