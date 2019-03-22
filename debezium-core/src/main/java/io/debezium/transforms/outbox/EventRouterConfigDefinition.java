/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.RegexValidator;

/**
 * Debezium Outbox Transform configuration definition
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouterConfigDefinition {

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
            if (value == null) return null;
            value = value.trim();
            for (InvalidOperationBehavior option : InvalidOperationBehavior.values()) {
                if (option.getValue().equalsIgnoreCase(value)) return option;
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static InvalidOperationBehavior parse(String value, String defaultValue) {
            InvalidOperationBehavior mode = parse(value);
            if (mode == null && defaultValue != null) mode = parse(defaultValue);
            return mode;
        }
    }

    static final Field FIELD_EVENT_ID = Field.create("table.field.event.id")
            .withDisplayName("Event ID Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("id")
            .withDescription("The column which contains the Event ID within the outbox table");

    static final Field FIELD_EVENT_KEY = Field.create("table.field.event.key")
            .withDisplayName("Event Key Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The column which contains the Event Key within the outbox table");

    static final Field FIELD_EVENT_TYPE = Field.create("table.field.event.type")
            .withDisplayName("Event Type Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("type")
            .withDescription("The column which contains the Event Type within the outbox table");

    static final Field FIELD_EVENT_TIMESTAMP = Field.create("table.field.event.timestamp")
            .withDisplayName("Event Timestamp Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optionally you can override the Kafka message timestamp with a value from a chosen" +
                    " field, otherwise it'll be the debezium event processed timestamp.");

    static final Field FIELD_PAYLOAD = Field.create("table.field.payload")
            .withDisplayName("Event Payload Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("payload")
            .withDescription("The column which contains the Event Type within the outbox table");

    static final Field FIELD_PAYLOAD_ID = Field.create("table.field.payload.id")
            .withDisplayName("Event Payload ID Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("aggregateid")
            .withDescription("The column which contains the Payload ID within the outbox table");

    static final Field ROUTE_BY_FIELD = Field.create("route.by.field")
            .withDisplayName("Field to route events by")
            .withType(ConfigDef.Type.STRING)
            .withDefault("aggregatetype")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The column which determines how the events will be routed, the value will become part of" +
                    " the topic name");

    static final Field ROUTE_TOPIC_REGEX = Field.create("route.topic.regex")
            .withDisplayName("The name of the routed topic")
            .withType(ConfigDef.Type.STRING)
            .withValidation(Field::isRegex)
            .withDefault("(?<routedByValue>.*)")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The default regex to use within the RegexRouter, the default capture will allow" +
                    " to replace the routed field into a new topic name defined in 'route.topic.replacement'");

    static final Field ROUTE_TOPIC_REPLACEMENT = Field.create("route.topic.replacement")
            .withDisplayName("The name of the routed topic")
            .withType(ConfigDef.Type.STRING)
            .withDefault("outbox.event.${routedByValue}")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic in which the events will be routed, a replacement" +
                    " '${routedByValue}' is available which is the value of The column configured" +
                    " via 'route.by.field'");

    static final Field OPERATION_INVALID_BEHAVIOR = Field.create("debezium.op.invalid.behavior")
            .withDisplayName("Behavior when the route fails to apply")
            .withEnum(InvalidOperationBehavior.class, InvalidOperationBehavior.SKIP_AND_WARN)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("While Debezium is monitoring the table, it's expecting only to see 'create' row events," +
                    " in case something else is processed this transform can log it as warning, error or stop the" +
                    " process");

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
                FIELD_EVENT_ID, FIELD_EVENT_KEY, FIELD_EVENT_TYPE, FIELD_PAYLOAD, FIELD_PAYLOAD_ID, FIELD_EVENT_TIMESTAMP
        );
        Field.group(
                config,
                "Router",
                ROUTE_BY_FIELD, ROUTE_TOPIC_REPLACEMENT
        );
        Field.group(
                config,
                "Debezium",
                OPERATION_INVALID_BEHAVIOR
        );
        return config;
    }
}
