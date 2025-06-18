/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cockroachdb.CockroachDBConnectorConfig;
import io.debezium.connector.cockroachdb.CockroachDBErrorHandler;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * A JDBC connection to CockroachDB, with retry-aware utilities for resilience.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnection.class);

    public CockroachDBConnection(JdbcConfiguration config, String purpose) {
        super(config, new FilteredConnectionFactory(), purpose);
    }

    public static CockroachDBConnection forConfig(CockroachDBConnectorConfig connectorConfig, String purpose) {
        return new CockroachDBConnection(connectorConfig.getJdbcConfig(), purpose);
    }

    public Connection connection(boolean autocommit) throws SQLException {
        Connection conn = connection();
        conn.setAutoCommit(autocommit);
        return conn;
    }

    public String username() {
        return jdbcConfig().getString(JdbcConfiguration.USER);
    }

    /**
     * Performs a test query to verify CockroachDB connectivity.
     * Retries once on transient SQL errors.
     */
    public void testConnection() throws SQLException {
        try {
            execute("SELECT version()");
            LOGGER.info("Successfully tested connection to CockroachDB.");
        }
        catch (SQLException e) {
            boolean retry = CockroachDBErrorHandler.handleAndLogRetry("testConnection", e);
            if (!retry) {
                throw e;
            }

            LOGGER.info("Retrying testConnection after transient failure...");
            try {
                Thread.sleep(500);
                execute("SELECT version()");
                LOGGER.info("Retry succeeded: testConnection");
            }
            catch (SQLException retryEx) {
                throw new SQLException("Retry attempt failed for testConnection", retryEx);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted during retry sleep", interrupted);
            }
        }
    }

    /**
     * Execute a SQL-producing function with retry on transient CockroachDB errors.
     */
    public <T> T withRetry(String operationDescription, SQLCallable<T> callable) throws SQLException {
        try {
            return callable.call();
        }
        catch (SQLException e) {
            if (!CockroachDBErrorHandler.handleAndLogRetry(operationDescription, e)) {
                throw e;
            }

            LOGGER.info("Retrying operation: {}", operationDescription);
            try {
                Thread.sleep(500);
                return callable.call();
            }
            catch (SQLException retryEx) {
                throw new SQLException("Retry failed for: " + operationDescription, retryEx);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted during retry", interrupted);
            }
        }
    }

    @FunctionalInterface
    public interface SQLCallable<T> {
        T call() throws SQLException;
    }

    private static class FilteredConnectionFactory implements JdbcConnection.ConnectionFactory {
        @Override
        public Connection connect(String jdbcUrl, Properties props) throws SQLException {
            LOGGER.debug("Connecting to: {}", jdbcUrl);
            return new org.postgresql.Driver().connect(jdbcUrl, props);
        }
    }
}
