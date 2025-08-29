/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.outbox;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;
import io.debezium.transforms.outbox.AdditionalFieldsValidator;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;
import io.debezium.transforms.tracing.ActivateTracingSpan;

/**
 * Debezium MongoDB Outbox Event Router SMT configuration definition
 *
 * @author Sungho Hwang
 */
public class MongoEventRouterConfigDefinition {
    static final Field FIELD_EVENT_ID = Field.create("collection.field.event.id")
            .withDisplayName("Event ID Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("_id")
            .withDescription("The field which contains the event ID within the outbox collection");

    static final Field FIELD_EVENT_KEY = Field.create("collection.field.event.key")
            .withDisplayName("Event Key Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The field which contains the event key within the outbox collection");

    static final Field FIELD_EVENT_TYPE = Field.create("collection.field.event.type")
            .withDisplayName("Event Type Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("type")
            .withDescription("The field which contains the event type within the outbox collection");

    static final Field FIELD_EVENT_TIMESTAMP = Field.create("collection.field.event.timestamp")
            .withDisplayName("Event Timestamp Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optionally you can override the Kafka message timestamp with a value from a chosen" +
                    " field, otherwise it'll be the Debezium event processed timestamp.");

    static final Field FIELD_PAYLOAD = Field.create("collection.field.event.payload")
            .withDisplayName("Event Payload Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("payload")
            .withDescription("The field which contains the event payload within the outbox collection");

    static final Field FIELDS_ADDITIONAL_PLACEMENT = Field.create("collection.fields.additional.placement")
            .withDisplayName("Settings for each additional column in the outbox table")
            .withType(ConfigDef.Type.LIST)
            .withValidation(AdditionalFieldsValidator::isListOfStringPairs)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Extra fields can be added as part of the event envelope or a message header, format" +
                    " is a list of colon-delimited pairs or trios when you desire to have aliases," +
                    " e.g. <code>id:header,field_name:envelope:alias</code> ");

    static final Field FIELD_SCHEMA_VERSION = Field.create("collection.field.event.schema.version")
            .withDisplayName("Event Schema Version Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The field which contains the event schema version within the outbox collection");

    static final Field ROUTE_BY_FIELD = EventRouterConfigDefinition.ROUTE_BY_FIELD
            .withDescription("The field which determines how the events will be routed within the outbox collection. The value will become a part of" +
                    " the topic name");

    static final Field ROUTE_TOPIC_REGEX = EventRouterConfigDefinition.ROUTE_TOPIC_REGEX;

    static final Field ROUTE_TOPIC_REPLACEMENT = EventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT
            .withDescription("The name of the topic in which the events will be routed, a replacement" +
                    " '${routedByValue}' is available which is the value of the field configured" +
                    " via 'route.by.field'");

    static final Field ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD = EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD;

    static final Field OPERATION_INVALID_BEHAVIOR = EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR
            .withDescription("While Debezium is monitoring the collection, it's expecting only to see 'create' document events," +
                    " in case something else is processed this transform can log it as warning, error or stop the" +
                    " process");

    static final Field EXPAND_JSON_PAYLOAD = Field.create("collection.expand.json.payload")
            .withDisplayName("Expand Payload escaped string as real JSON")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether or not to try unescaping a JSON string and make it real JSON. It will infer schema information" +
                    " from payload and update the record schema accordingly. If content is not JSON, it just produces a warning" +
                    " and emits the record unchanged");

    /**
     * There are 3 configuration groups available:
     * - Collection: Allows you to customize each of The field names in the outbox collection for your convenience
     * - Router: The behavior behind the events routing
     * - Debezium: Specific to Debezium behavior which might impact the transform
     *
     * @return ConfigDef
     */
    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();
        Field.group(
                config,
                "Collection",
                FIELD_EVENT_ID, FIELD_EVENT_KEY, FIELD_EVENT_TYPE, FIELD_PAYLOAD, FIELD_EVENT_TIMESTAMP, FIELDS_ADDITIONAL_PLACEMENT,
                FIELD_SCHEMA_VERSION, OPERATION_INVALID_BEHAVIOR, EXPAND_JSON_PAYLOAD);
        Field.group(
                config,
                "Router",
                ROUTE_BY_FIELD, ROUTE_TOPIC_REGEX, ROUTE_TOPIC_REPLACEMENT, ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD);
        Field.group(
                config,
                "Tracing",
                ActivateTracingSpan.TRACING_SPAN_CONTEXT_FIELD, ActivateTracingSpan.TRACING_OPERATION_NAME, ActivateTracingSpan.TRACING_CONTEXT_FIELD_REQUIRED);
        return config;
    }
}
