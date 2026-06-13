/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;

/**
 * Abstract base class for all the Oracle connector connection factories.
 *
 * @author Chris Cranford
 */
public abstract class OracleConnectionFactory implements MainConnectionProvidingConnectionFactory<OracleConnection> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectionFactory.class);

    /**
     * Get the connection factory that should be used during snapshot.
     * @return the connection factory, never {@code null}
     */
    public OracleConnectionFactory snapshotConnectionFactory() {
        return this;
    }

    /**
     * Get the connection factory that should be used during streaming.
     * @return the connection factory, never {@code null}
     */
    public OracleConnectionFactory streamingConnectionFactory() {
        return this;
    }

    /**
     * Validates all connections managed by this factory.
     *
     * @param validator the callback to validate each connection
     */
    public void validateConnections(ConnectionValidator validator) {
        validateConnection("primary", this, validator);
    }

    protected void validateConnection(String name, OracleConnectionFactory factory, ConnectionValidator validator) {
        try (OracleConnection connection = factory.newConnection()) {
            connection.getOracleVersion();
            LOGGER.debug("Successfully tested {} connection with user '{}'", name, connection.username());
        }
        catch (Exception e) {
            validator.onError(name, e);
        }
    }

    @FunctionalInterface
    public interface ConnectionValidator {
        void onError(String connectionName, Exception error);
    }

    /**
     * An internal decorator pattern to enforce connection read-only semantics.
     */
    protected static class ReadOnlyConnectionDecorator extends OracleConnection {
        public ReadOnlyConnectionDecorator(OracleConnectorConfig connectorConfig, boolean autoCommit) {
            super(connectorConfig, autoCommit);
        }

        public ReadOnlyConnectionDecorator(JdbcConfiguration jdbcConfiguration, boolean autoCommit) {
            super(jdbcConfiguration, autoCommit);
        }

        @Override
        public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
            final Connection connection = super.connection(executeOnConnect);
            connection.setReadOnly(true);
            return connection;
        }
    }

}
