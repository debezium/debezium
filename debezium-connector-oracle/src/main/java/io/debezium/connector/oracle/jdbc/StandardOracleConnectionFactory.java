/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.ConnectionFactory;

/**
 * Creates the default, standard Oracle connections that use the supplied {@code database.*} properties
 * for both the snapshot and streaming phases of the connector.
 *
 * @author Chris Cranford
 */
public class StandardOracleConnectionFactory extends OracleConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardOracleConnectionFactory.class);

    private final ConnectionFactory<OracleConnection> delegate;
    private final OracleConnection connection;

    public StandardOracleConnectionFactory(OracleConnectorConfig connectorConfig) {
        if (connectorConfig.isLogMiningReadOnly()) {
            LOGGER.info("Primary connection is read-only.");
            delegate = () -> new ReadOnlyConnectionDecorator(connectorConfig, false);
        }
        else {
            delegate = () -> new OracleConnection(connectorConfig, false);
        }

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

}
