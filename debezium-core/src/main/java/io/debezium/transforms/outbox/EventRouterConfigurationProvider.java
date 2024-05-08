/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.Map;

/**
 * Defines a contract allowing a connector to override specific Outbox configuration behavior.
 *
 * @author Chris Cranford
 */
public interface EventRouterConfigurationProvider {
    /**
     * Returns the module name associated with the configuration provider, typically connector name.
     */
    String getName();

    /**
     * Configures the value provider
     *
     * @param configMap the configuration, must never be {@literal null}.
     */
    void configure(Map<String, ?> configMap);

    /**
     * Get the {@literal FIELD_EVENT_ID} field name
     */
    String getFieldEventId();

    /**
     * Get the {@literal FIELD_EVENT_KEY} field name
     */
    String getFieldEventKey();

    /**
     * Get the {@literal FIELD_EVENT_TIMESTAMP} field name
     */
    String getFieldEventTimestamp();

    /**
     * Get the {@literal FIELD_PAYLOAD} field name
     */
    String getFieldPayload();

    /**
     * Get the {@literal ROUTE_BY_FIELD} field name
     */
    String getRouteByField();
}
