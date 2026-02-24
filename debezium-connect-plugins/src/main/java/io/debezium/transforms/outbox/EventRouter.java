/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.transforms.tracing.ActivateTracingSpan;

/**
 * Debezium Outbox Transform Event Router
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    EventRouterDelegate<R> eventRouterDelegate = new EventRouterDelegate<>();

    @Override
    public R apply(R r) {
        return eventRouterDelegate.apply(r, rec -> rec);
    }

    @Override
    public ConfigDef config() {
        return eventRouterDelegate.config();
    }

    @Override
    public void close() {
        eventRouterDelegate.close();
    }

    @Override
    public void configure(Map<String, ?> configMap) {
        eventRouterDelegate.configure(configMap);
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                EventRouterConfigDefinition.FIELD_EVENT_ID,
                EventRouterConfigDefinition.FIELD_EVENT_KEY,
                EventRouterConfigDefinition.FIELD_EVENT_TYPE,
                EventRouterConfigDefinition.FIELD_PAYLOAD,
                EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP,
                EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT,
                EventRouterConfigDefinition.FIELDS_ADDITIONAL_ERROR_ON_MISSING,
                EventRouterConfigDefinition.FIELD_SCHEMA_VERSION,
                EventRouterConfigDefinition.ROUTE_BY_FIELD,
                EventRouterConfigDefinition.ROUTE_TOPIC_REGEX,
                EventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT,
                EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD,
                EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR,
                EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD,
                EventRouterConfigDefinition.TABLE_JSON_PAYLOAD_NULL_BEHAVIOR,
                ActivateTracingSpan.TRACING_SPAN_CONTEXT_FIELD,
                ActivateTracingSpan.TRACING_OPERATION_NAME,
                ActivateTracingSpan.TRACING_CONTEXT_FIELD_REQUIRED);
    }
}
