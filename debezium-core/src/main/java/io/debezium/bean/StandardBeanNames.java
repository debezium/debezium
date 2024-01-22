/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bean;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.ValueConverterProvider;

/**
 * Contract that defines standard bean names.
 *
 * @author Chris Cranford
 */
public interface StandardBeanNames {
    /**
     * Name for the raw {@link Configuration} object.
     */
    String CONFIGURATION = Configuration.class.getName();

    /**
     * Name for the connector-specific configuration that extends {@link io.debezium.config.CommonConnectorConfig}.
     */
    String CONNECTOR_CONFIG = "ConnectorConfig";

    /**
     * The connector-specific database schema object name.
     */
    String DATABASE_SCHEMA = "Schema";

    /**
     * The connector-specific value converter object.
     */
    String VALUE_CONVERTER = ValueConverterProvider.class.getName();

    /**
     * The connector-specific JDBC connection implementation.
     */
    // Ideally we should consider exposing a connection factory contract that encapsulates the
    // all connector-specific connection details across all connectors (including MongoDB),
    // and then replace this name key as CONNECTION_FACTORY regardless of the connector.
    String JDBC_CONNECTION = JdbcConnection.class.getName();

    /**
     * The connector-specific offsets.
     */
    String OFFSETS = Offsets.class.getName();
}
