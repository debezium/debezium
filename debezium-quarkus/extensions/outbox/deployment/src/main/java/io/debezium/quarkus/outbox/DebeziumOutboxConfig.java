/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.outbox;

import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

/**
 *
 * Configuration root class for Debezium Outbox pattern that defines the available user
 * configuration options to customize this extension's behavior.
 *
 * @author Chris Cranford
 */
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public class DebeziumOutboxConfig {
    /**
     * The table name to be used for the outbox table
     */
    @ConfigItem(defaultValue = "OutboxEvent")
    public String tableName;

    /**
     * The column name that contains the event id in the outbox table
     */
    @ConfigItem(defaultValue = "id")
    public String columnIdName;

    /**
     * The column name that contains the event key within the outbox table
     */
    @ConfigItem(defaultValue = "aggregateid")
    public String aggregateIdName;

    /**
     * The column name that contains the event type in the outbox table
     */
    @ConfigItem(defaultValue = "type")
    public String typeName;

    /**
     * The column name that contains the timestamp in the outbox table
     */
    @ConfigItem(defaultValue = "timestamp")
    public String timestampName;

    /**
     * The column name that contains the event payload in the outbox table
     */
    @ConfigItem(defaultValue = "payload")
    public String payloadName;

    /**
     * The column name that determines how the events will be routed in the outbox table
     */
    @ConfigItem(defaultValue = "aggregatetype")
    public String aggregateTypeName;
}
