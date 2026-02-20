/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for {@link NumberOneToBooleanConverter}.
 */
public class NumberOneToBooleanConverterConfig {

    public static final Field SELECTOR = Field.create("selector")
            .withDisplayName("Column selector")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated list of column selectors (regular expressions) to match NUMBER(1) columns that should be converted to boolean. " +
                    "Format: <table_name>.<column_name>. Example: 'inventory.products.is_active,orders.*.is_processed'");
}
