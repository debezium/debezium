/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore;
import io.debezium.util.DelayStrategy;

/**
 * Class encapsulates a java.sql.Connection. It provides {@code executeWithRetry} method to execute code snippet
 * that interacts with the connection. If that fails with {@code SQLRecoverableException}, it will try to
 * re-create new connection and perform the complete code snippet again (first, it performs rollback
 * if specified in params).
 * It attempts to reconnect number of times as specified in
 * {@code io.debezium.storage.jdbc.JdbcCommonConfig.PROP_MAX_RETRIES} and there is a delay in between per
 * {@code io.debezium.storage.jdbc.JdbcCommonConfig.PROP_WAIT_RETRY_DELAY}
 *
 * The code snippet provided should handle commit of its own if required. The connection is marked as autocommit = false
 *
 * @author Jiri Kulhanek
 */
public class RetriableConnection implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcOffsetBackingStore.class);
    private final String url;
    private final String user;
    private final String pwd;
    private final Duration waitRetryDelay;
    private final int maxRetryCount;

    private Connection conn;

    public RetriableConnection(String url, String user, String pwd, Duration waitRetryDelay, int maxRetryCount) throws SQLException {
        this.url = url;
        this.user = user;
        this.pwd = pwd;
        this.waitRetryDelay = waitRetryDelay;
        this.maxRetryCount = maxRetryCount;

        try {
            createConnection();
        }
        catch (SQLException e) {
            LOGGER.error("Unable to create connection. It will be re-attempted during its first use: {}", e.getMessage(), e);
            close();
        }
    }

    private void createConnection() throws SQLException {
        this.conn = DriverManager.getConnection(url, user, pwd);
        this.conn.setAutoCommit(false);
    }

    @Override
    public void close() throws SQLException {
        if (isOpen()) {
            try {
                conn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing connection", e);
            }
        }
        conn = null;
    }

    public boolean isOpen() {
        try {
            return conn != null && !conn.isClosed();
        }
        catch (SQLException e) {
            LOGGER.warn("Exception while checking connection", e);
            conn = null;
        }
        return false;
    }

    /**
     * execute code snippet where no value is returned.
     * @param consumer code snippet to execute
     * @param name name of the operation being executed (for logging purposes)
     * @param rollback if set to true, the rollback will be called in case of SQLException
     * @throws SQLException sql connection related exception
     */
    public synchronized void executeWithRetry(ConnectionConsumer consumer, String name, boolean rollback)
            throws SQLException {
        executeWithRetry(null, consumer, name, rollback);
    }

    /**
     * execute code snippet which returns some value.
     * @param func code snippet to execute
     * @param name name of the operation being executed (for logging purposes)
     * @param rollback if set to true, the rollback will be called in case of SQLException
     * @throws SQLException sql connection related exception
     */
    public synchronized <T> T executeWithRetry(ConnectionFunction<T> func, String name, boolean rollback)
            throws SQLException {
        return executeWithRetry(func, null, name, rollback);
    }

    private synchronized <T> T executeWithRetry(ConnectionFunction<T> func, ConnectionConsumer consumer, String name, boolean rollback)
            throws SQLException {
        int attempt = 1;
        while (true) {
            if (!isOpen()) {
                LOGGER.debug("Trying to reconnect (attempt {}).", attempt);
                try {
                    createConnection();
                }
                catch (SQLException e) {
                    LOGGER.error("SQL Exception while trying to reconnect: {}", e.getMessage(), e);
                    close();
                    if (attempt >= maxRetryCount) {
                        throw e;
                    }
                    attempt++;
                    LOGGER.debug("Waiting for reconnect for {} ms.", waitRetryDelay);
                    DelayStrategy delayStrategy = DelayStrategy.constant(waitRetryDelay);
                    delayStrategy.sleepWhen(true);
                    continue;
                }
            }
            try {
                if (func != null) {
                    return func.accept(conn);
                }
                if (consumer != null) {
                    consumer.accept(conn);
                    return null;
                }
            }
            catch (SQLException e) {
                LOGGER.warn("Attempt {} to call '{}' failed.", attempt, name, e);
                if (rollback) {
                    LOGGER.warn("'{}': doing rollback.", name);
                    try {
                        conn.rollback();
                    }
                    catch (SQLException ex) {
                        // ignore
                    }
                }
                close();
            }
        }
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        /**
         * Performs this operation on the given connection.
         *
         * @param conn the input connection
         * @return result of the operation
         */
        T accept(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    public interface ConnectionConsumer {

        /**
         * Performs this operation on the given connection.
         *
         * @param conn the input connection
         */
        void accept(Connection conn) throws SQLException;

        /**
         * Returns a composed {@code ConnectionFunctionVoid} that performs, in sequence, this
         * operation followed by the {@code after} operation. If performing either
         * operation throws an exception, it is relayed to the caller of the
         * composed operation.  If performing this operation throws an exception,
         * the {@code after} operation will not be performed.
         *
         * @param after the operation to perform after this operation
         * @return a composed {@code ConnectionFunctionVoid} that performs in sequence this
         * operation followed by the {@code after} operation
         * @throws NullPointerException if {@code after} is null
         */
        default ConnectionConsumer andThen(ConnectionConsumer after) {
            Objects.requireNonNull(after);
            return (Connection c) -> {
                accept(c);
                after.accept(c);
            };
        }
    }

}
