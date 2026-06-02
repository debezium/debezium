/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import static io.debezium.jdbc.JdbcConfiguration.HOSTNAME;
import static io.debezium.jdbc.JdbcConfiguration.PORT;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.ConnectionFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Strings;

/**
 * Creates a connection factory that is designed to support snapshotting from the primary Oracle instance
 * and streaming changes from a read-only physical standby (replica).
 *
 * @author Chris Cranford
 */
public class PhysicalStandbyOracleConnectionFactory extends OracleConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalStandbyOracleConnectionFactory.class);

    private final ConnectionFactory<OracleConnection> primaryDelegate;
    private final OracleConnectionFactory standbyDelegate;
    private final OracleConnection primaryConnection;

    public PhysicalStandbyOracleConnectionFactory(OracleConnectorConfig connectorConfig) {
        if (connectorConfig.isLogMiningReadOnly()) {
            LOGGER.info("Primary connection is read-only.");
            primaryDelegate = () -> new ReadOnlyConnectionDecorator(connectorConfig, false);
        }
        else {
            primaryDelegate = () -> new OracleConnection(connectorConfig, false);
        }

        this.primaryConnection = primaryDelegate.newConnection();
        this.standbyDelegate = new StandbyConnectionFactory(connectorConfig);
    }

    @Override
    public OracleConnectionFactory streamingConnectionFactory() {
        return standbyDelegate;
    }

    @Override
    public void validateConnections(ConnectionValidator validator) {
        super.validateConnections(validator);
        validateConnection("standby", standbyDelegate, validator);
    }

    @Override
    public OracleConnection mainConnection() {
        // The outer connection factory always uses the primary
        // For standby connections, the streaming factory should be used
        return primaryConnection;
    }

    @Override
    public OracleConnection newConnection() {
        // The outer connection factory always uses the primary
        // For standby connections, the streaming factory should be used
        return primaryDelegate.newConnection();
    }

    /**
     * An internal factory that creates a connection to the physical standby instance in read-only mode
     * to stream changes. This is only used for streaming, never for snapshots nor metadata queries.
     */
    private static class StandbyConnectionFactory extends OracleConnectionFactory {
        private final ConnectionFactory<OracleConnection> delegate;
        private final OracleConnection connection;

        StandbyConnectionFactory(OracleConnectorConfig connectorConfig) {
            this.delegate = () -> new ReadOnlyConnectionDecorator(standbyConfig(connectorConfig), false);
            this.connection = delegate.newConnection();
        }

        @Override
        public OracleConnection mainConnection() {
            return connection;
        }

        @Override
        public OracleConnection newConnection() {
            return delegate.newConnection();
        }

        private JdbcConfiguration standbyConfig(OracleConnectorConfig connectorConfig) {
            final OracleJdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
            final Properties properties = jdbcConfig.asProperties();

            if (!Strings.isNullOrBlank(jdbcConfig.getStandbyUrl())) {
                properties.put(OracleJdbcConfiguration.URL.name(), jdbcConfig.getStandbyUrl());
            }
            else {
                if (!Strings.isNullOrEmpty(jdbcConfig.getStandbyHostName())) {
                    properties.put(HOSTNAME.name(), jdbcConfig.getStandbyHostName());
                }
            }

            properties.put(PORT.name(), String.valueOf(jdbcConfig.getStandbyPort()));

            properties.remove(OracleJdbcConfiguration.STANDBY_URL.name());
            properties.remove(OracleJdbcConfiguration.STANDBY_HOSTNAME.name());
            properties.remove(OracleJdbcConfiguration.STANDBY_PORT.name());

            LOGGER.info("Standby connection properties: {}", properties);
            return JdbcConfiguration.adapt(Configuration.from(properties));
        }
    }
}
