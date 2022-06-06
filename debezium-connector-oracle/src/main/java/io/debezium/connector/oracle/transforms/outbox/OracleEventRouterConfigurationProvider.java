/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.transforms.outbox;

import java.util.Map;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.oracle.Module;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;
import io.debezium.transforms.outbox.EventRouterConfigurationProvider;

/**
 * An implementation of the {@link EventRouterConfigurationProvider} for the Oracle connector.
 *
 * @author Chris Cranford
 */
public class OracleEventRouterConfigurationProvider implements EventRouterConfigurationProvider {

    private Configuration configuration;

    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public void configure(Map<String, ?> configMap) {
        this.configuration = Configuration.from(configMap);
    }

    @Override
    public String getFieldEventId() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_EVENT_ID);
    }

    @Override
    public String getFieldEventKey() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_EVENT_KEY);
    }

    @Override
    public String getFieldEventTimestamp() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP);
    }

    @Override
    public String getFieldPayload() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_PAYLOAD);
    }

    @Override
    public String getRouteByField() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.ROUTE_BY_FIELD);
    }

    private String getStringWithUpperCaseDefault(Field field) {
        if (configuration == null) {
            throw new DebeziumException("Event router configuration for Oracle has not yet been configured");
        }

        // Check if the configuration option is defined by the user and if so; use it.
        if (configuration.hasKey(field.name())) {
            return configuration.getString(field);
        }

        // Configuration option isn't defined by the user, use connector-specific fallback value.
        if (field.defaultValue() != null) {
            return field.defaultValueAsString().toUpperCase();
        }

        // no default value was supplied on the field
        return null;
    }
}
