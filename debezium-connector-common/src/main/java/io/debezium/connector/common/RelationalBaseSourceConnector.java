/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Strings;

/**
 * Base class for Debezium's relational CDC {@link BaseSourceConnector} implementations. Provides functionality common to
 * all relational CDC connectors, such as validation.
 */
public abstract class RelationalBaseSourceConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalBaseSourceConnector.class);

    private Map<String, String> connectorProps;

    @Override
    public void start(Map<String, String> props) {
        this.connectorProps = props;
    }

    /**
     * Enriches the given {@link ConfigDef} with per-table snapshot select statement override
     * properties as {@link org.apache.kafka.common.config.ConfigDef.Type#PASSWORD} when
     * connector properties are available (i.e. after {@link #start(Map)} has been called).
     *
     * @param configDef the base config definition to enrich
     * @return the enriched config definition
     */
    protected ConfigDef enrichConfigDef(ConfigDef configDef) {
        if (connectorProps != null) {
            RelationalDatabaseConnectorConfig.addSnapshotSelectOverridesToConfigDef(configDef, connectorProps);
        }
        return configDef;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);

        // Validate all the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);

        // Add per-table snapshot select statement override properties as PASSWORD type
        for (Field field : RelationalDatabaseConnectorConfig.getSnapshotSelectOverrideFields(connectorConfigs)) {
            ConfigValue configValue = new ConfigValue(field.name());
            configValue.value(connectorConfigs.get(field.name()));
            results.put(field.name(), configValue);
        }

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
}
