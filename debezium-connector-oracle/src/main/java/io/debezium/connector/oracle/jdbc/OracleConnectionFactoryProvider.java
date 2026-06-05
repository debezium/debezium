/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import static io.debezium.connector.oracle.jdbc.OracleJdbcConfiguration.SECONDARY_PREFIX;
import static io.debezium.connector.oracle.jdbc.OracleJdbcConfiguration.URL;
import static io.debezium.jdbc.JdbcConfiguration.DATABASE;
import static io.debezium.jdbc.JdbcConfiguration.HOSTNAME;
import static io.debezium.jdbc.JdbcConfiguration.PORT;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.ConnectionFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Strings;

/**
 * A provider that resolves the type of {@link OracleConnectionFactory} based on the supplied
 * connector configuration setup.
 *
 * @author Chris Cranford
 */
public final class OracleConnectionFactoryProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectionFactoryProvider.class);

    /**
     * Creates the connection factory based on configuration.
     *
     * @param connectorConfig connector configuration, must not be {@code null}
     * @return a usable connection factory for connector operations
     */
    public static OracleConnectionFactory create(OracleConnectorConfig connectorConfig) {
        switch (connectorConfig.getCaptureMode()) {
            case PHYSICAL_STANDBY -> {
                LOGGER.info("Using DUAL connection factory - Streams from Physical Standby");
                return new DualOracleConnectionFactory(connectorConfig, new ReadOnlySecondaryConnectionFactory(connectorConfig));
            }
            case DOWNSTREAM -> {
                LOGGER.info("Using DUAL connection factory - Streams from Downstream Mining Instance");
                return new DualOracleConnectionFactory(connectorConfig, new ReadOnlySecondaryConnectionFactory(connectorConfig));
            }
            default -> {
                LOGGER.info("Using STANDARD connection factory - Streams from Primary");
                return new StandardOracleConnectionFactory(connectorConfig);
            }
        }
    }

    /**
     * An internal factory for read-only streaming connections.
     * <p>
     * This creates a connection to the standby or downstream mining instance in read-only mode to stream changes.
     */
    private static class ReadOnlySecondaryConnectionFactory extends AbstractSecondaryConnectionFactory {
        private final ConnectionFactory<OracleConnection> delegate;
        private final OracleConnection connection;

        ReadOnlySecondaryConnectionFactory(OracleConnectorConfig connectorConfig) {
            this.delegate = () -> new ReadOnlyConnectionDecorator(secondaryConfig(connectorConfig), false);
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

    /**
     * Abstract base class for all secondary connection factories.
     */
    private static abstract class AbstractSecondaryConnectionFactory extends OracleConnectionFactory {
        protected JdbcConfiguration secondaryConfig(OracleConnectorConfig connectorConfig) {
            final OracleJdbcConfiguration connectorJdbcConfig = connectorConfig.getJdbcConfig();
            final Map<String, String> config = connectorJdbcConfig.asMap();

            if (!Strings.isNullOrEmpty(connectorJdbcConfig.getSecondaryDatabaseName())) {
                config.put(DATABASE.name(), connectorJdbcConfig.getSecondaryDatabaseName());
            }

            // When a URL is supplied, it takes precedence over the HOSTNAME
            // When both instances are on the same host, users can supply only 'database.hostname'
            if (!Strings.isNullOrEmpty(connectorJdbcConfig.getSecondaryUrl())) {
                config.put(URL.name(), connectorJdbcConfig.getSecondaryUrl());
            }
            else if (!Strings.isNullOrEmpty(connectorJdbcConfig.getSecondaryHostname())) {
                config.put(HOSTNAME.name(), connectorJdbcConfig.getSecondaryHostname());
            }

            if (connectorJdbcConfig.getSecondaryPort() > 0) {
                config.put(PORT.name(), String.valueOf(connectorJdbcConfig.getSecondaryPort()));
            }

            // Cleanup the secondary JDBC configuration
            config.keySet().removeIf(key -> key.toLowerCase().startsWith(SECONDARY_PREFIX));

            final Configuration secondaryConfig = Configuration.from(config);
            LOGGER.debug("Secondary connection properties: {}", secondaryConfig.withMaskedPasswords());

            return OracleJdbcConfiguration.adapt(secondaryConfig);
        }
    }
}
