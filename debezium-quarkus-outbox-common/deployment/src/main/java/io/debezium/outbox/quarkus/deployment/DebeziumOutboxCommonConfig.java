/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import java.util.Optional;

import io.debezium.outbox.quarkus.internal.OutboxConstants;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Configuration root class for Debezium Outbox pattern that defines the available user
 * configuration options to customize this extension's behavior.
 *
 * @author Chris Cranford
 */
@ConfigMapping(prefix = "quarkus.debezium-outbox")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface DebeziumOutboxCommonConfig {
    /**
     * The table name to be used for the outbox table
     */
    @WithDefault("OutboxEvent")
    String tableName();

    /**
     * Outbox identifier configurable attributes
     */
    DebeziumOutboxConfigId id();

    /**
     * Outbox aggregate-id configurable attributes
     */
    DebeziumOutboxConfigAggregateId aggregateId();

    /**
     * Outbox aggregate-type configurable attributes
     */
    DebeziumOutboxConfigAggregateType aggregateType();

    /**
     * Outbox type configurable attributes
     */
    DebeziumOutboxConfigType type();

    /**
     * Outbox timestamp configurable attributes
     */
    DebeziumOutboxConfigTimestamp timestamp();

    /**
     * Outbox payload configurable attributes
     */
    DebeziumOutboxConfigPayload payload();

    /**
     * Outbox additional fields
     */
    Optional<String> additionalFields();

    /**
     * Outbox Tracing configurable attributes
     */
    DebeziumOutboxConfigTracingSpan tracingSpan();

    /**
     * OpenTelemetry configuration option
     */
    @WithDefault("true")
    @WithName("tracing.enabled")
    boolean tracingEnabled();

    interface DebeziumOutboxConfigId {
        /**
         * The column name.
         */
        @WithDefault("id")
        String name();

        /**
         * The column definition.
         */
        Optional<String> columnDefinition();
    }

    interface DebeziumOutboxConfigAggregateType {
        /**
         * The column name.
         */
        @WithDefault("aggregatetype")
        String name();

        /**
         * The column definition.
         */
        Optional<String> columnDefinition();

        /**
         * The column's attribute converter fully qualified class name.
         * @see jakarta.persistence.AttributeConverter
         */
        Optional<String> converter();
    }

    interface DebeziumOutboxConfigAggregateId {
        /**
         * The column name.
         */
        @WithDefault("aggregateid")
        String name();

        /**
         * The column definition.
         */
        Optional<String> columnDefinition();

        /**
         * The column's attribute converter fully qualified class name.
         * @see jakarta.persistence.AttributeConverter
         */
        Optional<String> converter();
    }

    interface DebeziumOutboxConfigType {
        /**
         * The column name.
         */
        @WithDefault("type")
        String name();

        /**
         * The column definition.
         */
        Optional<String> columnDefinition();

        /**
         * The column's attribute converter fully qualified class name.
         * @see jakarta.persistence.AttributeConverter
         */
        Optional<String> converter();
    }

    interface DebeziumOutboxConfigTimestamp {
        /**
         * The column name.
         */
        @WithDefault("timestamp")
        String name();

        /**
         * The column definition.
         */
        Optional<String> columnDefinition();

        /**
         * The column's attribute converter fully qualified class name.
         * @see jakarta.persistence.AttributeConverter
         */
        Optional<String> converter();
    }

    interface DebeziumOutboxConfigPayload {
        /**
         * The column name.
         */
        @WithDefault("payload")
        String name();

        /**
         * The column definition.
         */
        Optional<String> columnDefinition();

        /**
         * The column's attribute converter fully qualified class name.
         * @see jakarta.persistence.AttributeConverter
         */
        Optional<String> converter();

        /**
         * The column's type definition class
         */
        Optional<String> type();
    }

    interface DebeziumOutboxConfigTracingSpan {
        /**
         * The column name.
         */
        @WithDefault(OutboxConstants.TRACING_SPAN_CONTEXT)
        String name();

        /**
         * The column definition.
         */
        Optional<String> columnDefinition();
    }
}
