/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.DelayStrategy;

/**
 * Establishes a new connection to JDBC
 *
 * @author Ilyas Ahsan
 */
public class JdbcConnection implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);

    private volatile Connection conn;
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final Duration waitRetryDelay;
    private final Integer maxRetryCount;

    public JdbcConnection(String jdbcUrl, String user, String password, Duration waitRetryDelay, int maxRetryCount) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.waitRetryDelay = waitRetryDelay;
        this.maxRetryCount = maxRetryCount;
        create();
    }

    public synchronized void execute(JdbcConnectionHandler handler, boolean rollback) {
        try {
            handler.apply(conn);
        }
        catch (SQLRecoverableException e) {
            handleRecoverableException(e);
        }
        catch (SQLException e) {
            LOGGER.error("Exception while executing SQL", e);
            throw new ConnectException("Failed to execute SQL", e);
        }
        finally {
            if (rollback) {
                rollback();
            }
        }
    }

    private synchronized void create() {
        try {
            conn = DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
            conn.setAutoCommit(false);
        }
        catch (SQLException e) {
            // ignore create connection exception
        }
    }

    @Override
    public synchronized void close() {
        try {
            conn.close();
        }
        catch (Exception e) {
            // ignore close connection exception
        }
    }

    private synchronized void rollback() {
        try {
            conn.rollback();
        }
        catch (SQLException e) {
            // ignore rollback exception
        }
    }

    private synchronized boolean isConnected() {
        int timeout = (int) waitRetryDelay.toMillis();
        try {
            return conn != null && conn.isValid(timeout);
        }
        catch (SQLException e) {
            LOGGER.error("Exception while checking connection", e);
            throw new ConnectException("Failed to check connection", e);
        }
    }

    private synchronized void handleRecoverableException(SQLException e) {
        DelayStrategy delayStrategy = null;
        int attempt = 0;
        while (!isConnected()) {
            attempt++;
            if (attempt > maxRetryCount) {
                LOGGER.error("Failed to execute SQL after {} attempts", attempt);
                throw new ConnectException("Failed to execute SQL after " + attempt + " attempts", e);
            }
            LOGGER.warn("Failed to execute SQL, retrying after {} ms", waitRetryDelay.toMillis());
            delayStrategy = DelayStrategy.constant(waitRetryDelay);
            delayStrategy.sleepWhen(true);
            close();
            create();
        }
    }

    @FunctionalInterface
    public interface JdbcConnectionHandler {
        void apply(Connection connection) throws SQLException;
    }
}
