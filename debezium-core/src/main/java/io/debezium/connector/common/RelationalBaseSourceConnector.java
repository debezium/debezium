/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.rest.model.DataCollection;
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

        // Validate all the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);

        if (Strings.isNullOrEmpty(config.getString(RelationalDatabaseConnectorConfig.PASSWORD))) {
            LOGGER.debug("The connection password is empty");
        }

        // Only if there are no config errors ...
        if (results.values().stream().allMatch(configValue -> configValue.errorMessages().isEmpty())) {
            // ... validate the connection too
            validateConnection(results, config);
        }

        return new Config(new ArrayList<>(results.values()));
    }

    /**
     * Validates connection to database.
     */
    protected abstract void validateConnection(Map<String, ConfigValue> configValues, Configuration config);

    protected abstract Map<String, ConfigValue> validateAllFields(Configuration config);

    public abstract List<DataCollection> getMatchingCollections(Configuration config);
}
