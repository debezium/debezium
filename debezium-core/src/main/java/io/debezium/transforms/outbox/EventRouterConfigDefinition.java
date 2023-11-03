/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.transforms.tracing.ActivateTracingSpan;

/**
 * Debezium Outbox Transform configuration definition
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouterConfigDefinition {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventRouterConfigDefinition.class);

    public enum InvalidOperationBehavior implements EnumeratedValue {
        SKIP_AND_WARN("warn"),
        SKIP_AND_ERROR("error"),
        FATAL("fatal");

        private final String value;

        InvalidOperationBehavior(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static InvalidOperationBehavior parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (InvalidOperationBehavior option : InvalidOperationBehavior.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public enum AdditionalFieldPlacement implements EnumeratedValue {
        HEADER("header"),
        ENVELOPE("envelope"),
        PARTITION("partition");

        private final String value;

        AdditionalFieldPlacement(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static AdditionalFieldPlacement parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (AdditionalFieldPlacement option : AdditionalFieldPlacement.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public enum JsonPayloadNullFieldBehavior implements EnumeratedValue {
        IGNORE("ignore"),
        OPTIONAL_BYTES("optional_bytes");

        private final String value;

        JsonPayloadNullFieldBehavior(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static JsonPayloadNullFieldBehavior parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (JsonPayloadNullFieldBehavior option : JsonPayloadNullFieldBehavior.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public static class AdditionalField {
        private final AdditionalFieldPlacement placement;
        private final String field;
        private final String alias;

        AdditionalField(AdditionalFieldPlacement placement, String field, String alias) {
            this.placement = placement;
            this.field = field;
            this.alias = alias;
        }

        public AdditionalFieldPlacement getPlacement() {
            return placement;
        }

        public String getField() {
            return field;
        }

        public String getAlias() {
            return alias;
        }
    }

    public static final Field OPERATION_INVALID_BEHAVIOR = Field.create("table.op.invalid.behavior")
            .withDisplayName("Behavior when capturing an unexpected outbox event")
            .withEnum(InvalidOperationBehavior.class, InvalidOperationBehavior.SKIP_AND_WARN)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("While Debezium is capturing changes from the outbox table, it is expecting only to process 'create' or 'delete' row events;" +
                    " in case something else is processed this transform can log it as warning, error or stop the" +
                    " process.");

    public static final Field FIELD_EVENT_ID = Field.create("table.field.event.id")
            .withDisplayName("Event ID Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("id")
            .withDescription("The column which contains the event ID within the outbox table");

    public static final Field FIELD_EVENT_KEY = Field.create("table.field.event.key")
            .withDisplayName("Event Key Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("aggregateid")
            .withDescription("The column which contains the event key within the outbox table");

    public static final Field FIELD_EVENT_TYPE = Field.create("table.field.event.type")
            .withDisplayName("Event Type Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("type")
            .withDescription("The column which contains the event type within the outbox table");

    public static final Field FIELD_EVENT_TIMESTAMP = Field.create("table.field.event.timestamp")
            .withDisplayName("Event Timestamp Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optionally you can override the Kafka message timestamp with a value from a chosen" +
                    " column, otherwise it'll be the Debezium event processed timestamp.");

    public static final Field FIELD_PAYLOAD = Field.create("table.field.event.payload")
            .withDisplayName("Event Payload Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("payload")
            .withDescription("The column which contains the event payload within the outbox table");

    public static final Field FIELDS_ADDITIONAL_PLACEMENT = Field.create("table.fields.additional.placement")
            .withDisplayName("Settings for each additional column in the outbox table")
            .withType(ConfigDef.Type.LIST)
            .withValidation(AdditionalFieldsValidator::isListOfStringPairs)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Extra fields can be added as part of the event envelope or a message header, format" +
                    " is a list of colon-delimited pairs or trios when you desire to have aliases," +
                    " e.g. <code>id:header,field_name:envelope:alias,field_name:partition</code> ");

    public static final Field FIELDS_ADDITIONAL_ERROR_ON_MISSING = Field.create("table.fields.additional.error.on.missing")
            .withDisplayName("Should the transform error if an additional field is missing in the change data")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("When transforming the 'table.fields.additional.placement' fields, should the transform throw" +
                    " an exception if one of the fields is missing in the change data");

    public static final Field FIELD_SCHEMA_VERSION = Field.create("table.field.event.schema.version")
            .withDisplayName("Event Schema Version Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The column which contains the event schema version within the outbox table");

    public static final Field ROUTE_BY_FIELD = Field.create("route.by.field")
            .withDisplayName("Field to route events by")
            .withType(ConfigDef.Type.STRING)
            .withDefault("aggregatetype")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The column which determines how the events will be routed, the value will become part of" +
                    " the topic name");

    public static final Field ROUTE_TOPIC_REGEX = Field.create("route.topic.regex")
            .withDisplayName("The name of the routed topic")
            .withType(ConfigDef.Type.STRING)
            .withValidation(Field::isRegex)
            .withDefault("(?<routedByValue>.*)")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The default regex to use within the RegexRouter, the default capture will allow" +
                    " to replace the routed field into a new topic name defined in 'route.topic.replacement'");

    public static final Field ROUTE_TOPIC_REPLACEMENT = Field.create("route.topic.replacement")
            .withDisplayName("The name of the routed topic")
            .withType(ConfigDef.Type.STRING)
            .withDefault("outbox.event.${routedByValue}")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic in which the events will be routed, a replacement" +
                    " '${routedByValue}' is available which is the value of The column configured" +
                    " via 'route.by.field'");

    public static final Field ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD = Field.create("route.tombstone.on.empty.payload")
            .withDisplayName("Empty payloads cause a tombstone message")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Whether or not an empty payload should cause a tombstone event.");

    public static final Field EXPAND_JSON_PAYLOAD = Field.create("table.expand.json.payload")
            .withDisplayName("Expand Payload escaped string as real JSON")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether or not to try unescaping a JSON string and make it real JSON. It will infer schema information" +
                    " from payload and update the record schema accordingly. If content is not JSON, it just produces a warning" +
                    " and emits the record unchanged");

    public static final Field TABLE_JSON_PAYLOAD_NULL_BEHAVIOR = Field.create("table.json.payload.null.behavior")
            .withDisplayName("Behavior when json payload including null value")
            .withEnum(JsonPayloadNullFieldBehavior.class, JsonPayloadNullFieldBehavior.IGNORE)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Behavior when json payload including null value, the default will ignore null, optional_bytes" +
                    " will keep the null value, and treat null as optional bytes of connect.");

    static final Field[] CONFIG_FIELDS = {
            FIELD_EVENT_ID,
            FIELD_EVENT_KEY,
            FIELD_EVENT_TYPE,
            FIELD_PAYLOAD,
            FIELD_EVENT_TIMESTAMP,
            FIELDS_ADDITIONAL_PLACEMENT,
            FIELDS_ADDITIONAL_ERROR_ON_MISSING,
            FIELD_SCHEMA_VERSION,
            ROUTE_BY_FIELD,
            ROUTE_TOPIC_REGEX,
            ROUTE_TOPIC_REPLACEMENT,
            ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD,
            OPERATION_INVALID_BEHAVIOR,
            EXPAND_JSON_PAYLOAD,
            TABLE_JSON_PAYLOAD_NULL_BEHAVIOR
    };

    /**
     * There are 3 configuration groups available:
     * - Table: Allows you to customize each of The column names in the outbox table for your convenience
     * - Router: The behavior behind the events routing
     * - Debezium: Specific to Debezium behavior which might impact the transform
     *
     * @return ConfigDef
     */
    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();
        Field.group(
                config,
                "Table",
                FIELD_EVENT_ID, FIELD_EVENT_KEY, FIELD_EVENT_TYPE, FIELD_PAYLOAD, FIELD_EVENT_TIMESTAMP, FIELDS_ADDITIONAL_PLACEMENT,
                FIELDS_ADDITIONAL_ERROR_ON_MISSING, FIELD_SCHEMA_VERSION, OPERATION_INVALID_BEHAVIOR, EXPAND_JSON_PAYLOAD, TABLE_JSON_PAYLOAD_NULL_BEHAVIOR);
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

    static List<AdditionalField> parseAdditionalFieldsConfig(Configuration config) {
        String extraFieldsMapping = config.getString(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT);
        List<AdditionalField> additionalFields = new ArrayList<>();

        if (extraFieldsMapping != null) {
            for (String field : extraFieldsMapping.split(",")) {
                final String[] parts = field.split(":");
                final String fieldName = parts[0];
                AdditionalFieldPlacement placement = AdditionalFieldPlacement.parse(parts[1]);
                final AdditionalField addField = new AdditionalField(placement, fieldName, parts.length == 3 ? parts[2] : fieldName);
                additionalFields.add(addField);
            }
        }

        return additionalFields;
    }
}
