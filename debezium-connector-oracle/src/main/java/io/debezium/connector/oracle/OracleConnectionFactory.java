/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.jdbc.ConnectionFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.util.Strings;

/**
 * Factory to manage between read-only and regular connection for the purpose of using only what is necessary for
 * start the connector while the read-only will be used to log mining.

 * @author Lucas Gazire
 * @author Chris Cranford
 */
public class OracleConnectionFactory implements MainConnectionProvidingConnectionFactory<OracleConnection> {

    private final ConnectionFactory<OracleConnection> mainConnectionFactory;
    private final ConnectionFactory<OracleConnection> readOnlyConnectionFactory;
    private final OracleConnection mainConnection;
    private final OracleConnection readOnlyConnection;

    public OracleConnectionFactory(OracleConnectorConfig connectorConfig) {
        this.mainConnectionFactory = () -> new OracleConnection(connectorConfig.getJdbcConfig());
        this.readOnlyConnectionFactory = buildReadOnlyConnectionFactory(connectorConfig);
        this.mainConnection = mainConnectionFactory.newConnection();
        this.readOnlyConnection = readOnlyConnectionFactory != null ? readOnlyConnectionFactory.newConnection() : null;
    }

    public OracleConnection getConnection() {
        return readOnlyConnection != null ? readOnlyConnection : mainConnection;
    }

    @Override
    public OracleConnection mainConnection() {
        return mainConnection;
    }

    @Override
    public OracleConnection newConnection() {
        return mainConnectionFactory.newConnection();
    }

    private static ConnectionFactory<OracleConnection> buildReadOnlyConnectionFactory(OracleConnectorConfig connectorConfig) {
        return connectorConfig.isLogMiningReadOnly() ? new ReadOnlyConnectionFactory(connectorConfig) : null;
    }

    /**
     * A connection factory that produces {@link OracleConnection} instances that are explicitly marked
     * to use a read-only connection.
     */
    private static class ReadOnlyConnectionFactory implements ConnectionFactory<OracleConnection> {

        private final ConnectionFactory<OracleConnection> delegate;

        ReadOnlyConnectionFactory(OracleConnectorConfig connectorConfig) {
            this.delegate = () -> new OracleConnection(buildReadOnlyConfig(connectorConfig)) {
                @Override
                public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
                    final Connection connection = super.connection(executeOnConnect);
                    connection.setReadOnly(true);
                    return connection;
                }
            };
        }

        @Override
        public OracleConnection newConnection() {
            return delegate.newConnection();
        }

        private static JdbcConfiguration buildReadOnlyConfig(OracleConnectorConfig connectorConfig) {
            if (Strings.isNullOrEmpty(connectorConfig.getReadonlyHostname())) {
                throw new DebeziumException("Cannot create read only connection, read only hostname is empty");
            }

            final Map<String, String> jdbcConfig = connectorConfig.getJdbcConfig().asMap();
            jdbcConfig.put("hostname", connectorConfig.getReadonlyHostname());
            return JdbcConfiguration.adapt(Configuration.from(jdbcConfig));
        }
    }
}