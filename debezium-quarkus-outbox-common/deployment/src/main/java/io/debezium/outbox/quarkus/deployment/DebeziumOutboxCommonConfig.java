/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import java.util.Optional;

import io.debezium.outbox.quarkus.internal.OutboxConstants;
import io.quarkus.runtime.annotations.ConfigGroup;
import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

/**
 * Configuration root class for Debezium Outbox pattern that defines the available user
 * configuration options to customize this extension's behavior.
 *
 * @author Chris Cranford
 */
@ConfigRoot(phase = ConfigPhase.BUILD_TIME, name = "debezium-outbox")
public class DebeziumOutboxCommonConfig {
    /**
     * The table name to be used for the outbox table
     */
    @ConfigItem(defaultValue = "OutboxEvent")
    public String tableName;

    /**
     * Outbox identifier configurable attributes
     */
    @ConfigItem
    public DebeziumOutboxConfigId id;

    /**
     * Outbox aggregate-id configurable attributes
     */
    @ConfigItem
    public DebeziumOutboxConfigAggregateId aggregateId;

    /**
     * Outbox aggregate-type configurable attributes
     */
    @ConfigItem
    public DebeziumOutboxConfigAggregateType aggregateType;

    /**
     * Outbox type configurable attributes
     */
    @ConfigItem
    public DebeziumOutboxConfigType type;

    /**
     * Outbox timestamp configurable attributes
     */
    @ConfigItem
    public DebeziumOutboxConfigTimestamp timestamp;

    /**
     * Outbox payload configurable attributes
     */
    @ConfigItem
    public DebeziumOutboxConfigPayload payload;

    /**
     * Outbox additional fields
     */
    @ConfigItem
    public Optional<String> additionalFields;

    /**
     * Outbox Tracing configurable attributes
     */
    @ConfigItem
    public DebeziumOutboxConfigTracingSpan tracingSpan;

    /**
     * smallrye-opentracing configuration option
     */
    @ConfigItem(name = "tracing.enabled", defaultValue = "true")
    public boolean tracingEnabled;

    @ConfigGroup
    public static class DebeziumOutboxConfigId {
        /**
         * The column name.
         */
        @ConfigItem(defaultValue = "id")
        public String name;

        /**
         * The column definition.
         */
        @ConfigItem
        public Optional<String> columnDefinition;
    }

    @ConfigGroup
    public static class DebeziumOutboxConfigAggregateType {
        /**
         * The column name.
         */
        @ConfigItem(defaultValue = "aggregatetype")
        public String name;

        /**
         * The column definition.
         */
        @ConfigItem
        public Optional<String> columnDefinition;

        /**
         * The column's attribute converter fully qualified class name.
         * @see javax.persistence.AttributeConverter
         */
        @ConfigItem
        public Optional<String> converter;
    }

    @ConfigGroup
    public static class DebeziumOutboxConfigAggregateId {
        /**
         * The column name.
         */
        @ConfigItem(defaultValue = "aggregateid")
        public String name;

        /**
         * The column definition.
         */
        @ConfigItem
        public Optional<String> columnDefinition;

        /**
         * The column's attribute converter fully qualified class name.
         * @see javax.persistence.AttributeConverter
         */
        @ConfigItem
        public Optional<String> converter;
    }

    @ConfigGroup
    public static class DebeziumOutboxConfigType {
        /**
         * The column name.
         */
        @ConfigItem(defaultValue = "type")
        public String name;

        /**
         * The column definition.
         */
        @ConfigItem
        public Optional<String> columnDefinition;

        /**
         * The column's attribute converter fully qualified class name.
         * @see javax.persistence.AttributeConverter
         */
        @ConfigItem
        public Optional<String> converter;
    }

    @ConfigGroup
    public static class DebeziumOutboxConfigTimestamp {
        /**
         * The column name.
         */
        @ConfigItem(defaultValue = "timestamp")
        public String name;

        /**
         * The column definition.
         */
        @ConfigItem
        public Optional<String> columnDefinition;

        /**
         * The column's attribute converter fully qualified class name.
         * @see javax.persistence.AttributeConverter
         */
        @ConfigItem
        public Optional<String> converter;
    }

    @ConfigGroup
    public static class DebeziumOutboxConfigPayload {
        /**
         * The column name.
         */
        @ConfigItem(defaultValue = "payload")
        public String name;

        /**
         * The column definition.
         */
        @ConfigItem
        public Optional<String> columnDefinition;

        /**
         * The column's attribute converter fully qualified class name.
         * @see javax.persistence.AttributeConverter
         */
        @ConfigItem
        public Optional<String> converter;

        /**
         * The column's type definition class
         */
        @ConfigItem
        public Optional<String> type;
    }

    @ConfigGroup
    public static class DebeziumOutboxConfigTracingSpan {
        /**
         * The column name.
         */
        @ConfigItem(defaultValue = OutboxConstants.TRACING_SPAN_CONTEXT)
        public String name;

        /**
         * The column definition.
         */
        @ConfigItem
        public Optional<String> columnDefinition;
    }
}
