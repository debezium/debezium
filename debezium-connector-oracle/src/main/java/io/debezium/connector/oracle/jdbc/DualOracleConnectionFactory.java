/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import io.debezium.connector.oracle.OracleConnectorConfig;

/**
 * Creates a connection factory that is designed to support snapshotting from the primary Oracle instance
 * and streaming changes from a secondary system.
 *
 * @author Chris Cranford
 */
public class DualOracleConnectionFactory extends StandardOracleConnectionFactory {

    private final OracleConnectionFactory secondaryDelegate;

    public DualOracleConnectionFactory(OracleConnectorConfig connectorConfig, OracleConnectionFactory secondaryFactory) {
        super(connectorConfig);
        this.secondaryDelegate = secondaryFactory;
    }

    @Override
    public OracleConnectionFactory streamingConnectionFactory() {
        return secondaryDelegate;
    }

    @Override
    public void validateConnections(ConnectionValidator validator) {
        super.validateConnections(validator);
        validateConnection("secondary", secondaryDelegate, validator);
    }

}
