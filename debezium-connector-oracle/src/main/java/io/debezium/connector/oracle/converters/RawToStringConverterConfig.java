/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for {@link RawToStringConverter}.
 */
public class RawToStringConverterConfig {

    public static final Field SELECTOR = Field.create("selector")
            .withDisplayName("Column selector")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated list of column selectors (regular expressions) to match columns that should be converted. " +
                    "Format: <table_name>.<column_name>. Example: 'inventory.products.metadata,orders.*.data'");

    public static final Field CHARSET = Field.create("charset")
            .withDisplayName("Character set")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault("UTF-8")
            .withDescription("The character set used to decode RAW column bytes into strings. " +
                    "Defaults to UTF-8. For databases using a non-UTF-8 character set such as WE8ISO8859P1, " +
                    "this should be set to the corresponding Java charset name (e.g. 'ISO-8859-1').");
}
