/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

public class TimescaleDbConfigDefinition {

    public static final String SCHEMA_LIST_NAMES_CONF = "schema.list";
    public static final String SCHEMA_LIST_NAMES_DEFAULT = "_timescaledb_internal";

    public static final String TARGET_TOPIC_PREFIX_CONF = "target.topic.prefix";
    public static final String TARGET_TOPIC_PREFIX_DEFAULT = "timescaledb";

    static final Field SCHEMA_LIST_NAMES_FIELD = Field.create(SCHEMA_LIST_NAMES_CONF)
            .withDisplayName("The list of TimescaleDB data schemas")
            .withType(ConfigDef.Type.LIST)
            .withDefault(SCHEMA_LIST_NAMES_DEFAULT)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated list schema names that contain TimescaleDB data tables, defaults to: '" + SCHEMA_LIST_NAMES_DEFAULT + "'");

    static final Field TARGET_TOPIC_PREFIX_FIELD = Field.create(TARGET_TOPIC_PREFIX_CONF)
            .withDisplayName("The prefix of TimescaleDB topic names")
            .withType(ConfigDef.Type.STRING)
            .withDefault(TARGET_TOPIC_PREFIX_DEFAULT)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The namespace (prefix) of topics where TimescaleDB events will be routed, defaults to: '" + SCHEMA_LIST_NAMES_DEFAULT + "'");
}
