/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
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
        if (resolveDeploymentMode(connectorConfig) == DeploymentMode.PHYSICAL_STANDBY) {
            LOGGER.info("Creating PHYSICAL STANDBY connection factory");
            return new PhysicalStandbyOracleConnectionFactory(connectorConfig);
        }
        LOGGER.info("Creating STANDARD connection factory");
        return new StandardOracleConnectionFactory(connectorConfig);
    }

    private static DeploymentMode resolveDeploymentMode(OracleConnectorConfig connectorConfig) {
        final OracleJdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
        if (!Strings.isNullOrBlank(jdbcConfig.getStandbyHostName()) || !Strings.isNullOrEmpty(jdbcConfig.getStandbyUrl())) {
            return DeploymentMode.PHYSICAL_STANDBY;
        }
        return DeploymentMode.STANDARD;
    }
}
