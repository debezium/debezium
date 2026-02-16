/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for {@link JdbcSinkDataTypesConverter}.
 */
public class JdbcSinkDataTypesConverterConfig {

    public static final Field SELECTOR_BOOLEAN = Field.create("selector.boolean")
            .withDisplayName("Boolean column selector")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated list of column selectors for BOOLEAN columns. " +
                    "These will be emitted as INT16 (true=1, false=0).");

    public static final Field SELECTOR_REAL = Field.create("selector.real")
            .withDisplayName("Real column selector")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated list of column selectors for REAL columns. " +
                    "These will be emitted as FLOAT32 or FLOAT64 based on treat.real.as.double setting.");

    public static final Field SELECTOR_STRING = Field.create("selector.string")
            .withDisplayName("String column selector")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated list of column selectors for string columns. " +
                    "These will include character set information in the schema.");

    public static final Field TREAT_REAL_AS_DOUBLE = Field.create("treat.real.as.double")
            .withDisplayName("Treat REAL as DOUBLE")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("If true (MySQL default), REAL columns are emitted as FLOAT64. " +
                    "If false, they are emitted as FLOAT32.");
}
