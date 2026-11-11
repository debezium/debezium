/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for {@link ZeroDateFallbackConverter}.<p></p>
 *
 * Column-level overrides use a dynamic property key ({@code column.<schema>.<table>.<column>.fallback})
 * and are looked up directly from the raw properties, so they do not have a static {@link Field}.
 *
 * @author Chris Cranford
 */
public class ZeroDateFallbackConverterConfig {

    public static final String DEFAULT_TARGET_TYPES = "DATE,TIMESTAMP";
    public static final String DEFAULT_SELECTOR = ".*";

    public static final Field TARGET_TYPES = Field.create("target.types")
            .withDisplayName("Target column types")
            .withType(ConfigDef.Type.STRING)
            .withDefault(DEFAULT_TARGET_TYPES)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Comma-separated list of Oracle column types this converter applies to. " +
                    "Allowed tokens: DATE, TIMESTAMP. Default is to apply to both.");

    public static final Field FALLBACK_DATE = Field.create("fallback.date")
            .withDisplayName("DATE zero-date fallback")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Replacement value emitted for Oracle DATE zero dates (0000-00-00 00:00:00) and " +
                    "absent values. Use 'NULL' (or leave unset) to emit null and make the column schema optional. " +
                    "Otherwise specify a UTC literal in the form yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.");

    public static final Field FALLBACK_TIMESTAMP = Field.create("fallback.timestamp")
            .withDisplayName("TIMESTAMP zero-date fallback")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Replacement value emitted for Oracle TIMESTAMP zero dates and absent values. " +
                    "Use 'NULL' (or leave unset) to emit null and make the column schema optional. " +
                    "Otherwise specify a UTC literal in the form yyyy-MM-dd or yyyy-MM-dd HH:mm:ss[.fffffffff].");

    public static final Field SELECTOR = Field.create("selector")
            .withDisplayName("Column selector")
            .withType(ConfigDef.Type.STRING)
            .withDefault(DEFAULT_SELECTOR)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Comma-separated list of regular expressions matched against " +
                    "<database>.<schema>.<table>.<column>. Only matching columns have this converter applied. " +
                    "Default '.*' matches every column whose type is in target.types.");

    public static final String COLUMN_FALLBACK_PREFIX = "column.";
    public static final String COLUMN_FALLBACK_SUFFIX = ".fallback";

    private ZeroDateFallbackConverterConfig() {
    }
}
