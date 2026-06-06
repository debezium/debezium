/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for {@link TinyIntOneToBooleanConverter}.
 */
public class TinyIntOneToBooleanConverterConfig {

    public static final Field SELECTOR = Field.create("selector")
            .withDisplayName("Column selector")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated list of column selectors (regular expressions) to match TINYINT(1) columns that should be converted to boolean. " +
                    "Format: <table_name>.<column_name>. Example: 'inventory.products.is_active,orders.*.is_processed'");

    public static final Field LENGTH_CHECKER = Field.create("length.checker")
            .withDisplayName("Enable length checking")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("If true, only TINYINT columns with length 1 will be converted to boolean. " +
                    "Set to false for MySQL 8+ where SHOW CREATE TABLE doesn't show length for TINYINT UNSIGNED.");
}
