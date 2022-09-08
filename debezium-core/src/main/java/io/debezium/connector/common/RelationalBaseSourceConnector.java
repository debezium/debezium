/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Strings;

/**
 * Base class for Debezium's relational CDC {@link SourceConnector} implementations. Provides functionality common to
 * all relational CDC connectors, such as validation.
 */
public abstract class RelationalBaseSourceConnector extends SourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalBaseSourceConnector.class);

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);

        // Validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);

        ConfigValue logicalName = results.get(CommonConnectorConfig.TOPIC_PREFIX.name());
        // Get the config values for each of the connection-related fields ...
        ConfigValue hostnameValue = results.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        ConfigValue portValue = results.get(RelationalDatabaseConnectorConfig.PORT.name());
        ConfigValue userValue = results.get(RelationalDatabaseConnectorConfig.USER.name());
        ConfigValue passwordValue = results.get(RelationalDatabaseConnectorConfig.PASSWORD.name());
        final String passwordStringValue = config.getString(RelationalDatabaseConnectorConfig.PASSWORD);

        if (Strings.isNullOrEmpty(passwordStringValue)) {
            LOGGER.debug("The connection password is empty");
        }

        // If there are no errors on any of these ...
        if (logicalName.errorMessages().isEmpty()
                && hostnameValue.errorMessages().isEmpty()
                && portValue.errorMessages().isEmpty()
                && userValue.errorMessages().isEmpty()
                && passwordValue.errorMessages().isEmpty()) {
            validateConnection(results, config);
        }

        return new Config(new ArrayList<>(results.values()));
    }

    /**
     * Validates connection to database.
     */
    protected abstract void validateConnection(Map<String, ConfigValue> configValues, Configuration config);

    protected abstract Map<String, ConfigValue> validateAllFields(Configuration config);
}
