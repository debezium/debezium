/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for {@link ZeroDateFallbackConverter}.
 * <p>
 * Type-level fallbacks and the target-types filter are exposed as proper {@link Field}s.
 * Column-level overrides use a dynamic property key
 * ({@code column.<dataCollection>.<column>.fallback}) and are looked up directly from the
 * raw {@link java.util.Properties} passed to {@link ZeroDateFallbackConverter#configure}, so
 * they do not need a static {@link Field} definition.
 */
public class ZeroDateFallbackConverterConfig {

    public static final String DEFAULT_TARGET_TYPES = "DATE,DATETIME,TIMESTAMP";
    public static final String DEFAULT_SELECTOR = ".*";

    public static final Field TARGET_TYPES = Field.create("target.types")
            .withDisplayName("Target JDBC types")
            .withType(ConfigDef.Type.STRING)
            .withDefault(DEFAULT_TARGET_TYPES)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Comma-separated list of JDBC temporal types this converter applies to. " +
                    "Allowed tokens: DATE, DATETIME, TIMESTAMP. Default is to apply to all three.");

    public static final Field FALLBACK_DATE = Field.create("fallback.date")
            .withDisplayName("DATE zero-date fallback")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Replacement value emitted for MySQL/MariaDB DATE zero-date rows " +
                    "(0000-00-00). Use 'NULL' (or leave unset) to emit null and promote the column " +
                    "schema to optional. Otherwise specify an ISO-8601 date literal (yyyy-MM-dd).");

    public static final Field FALLBACK_DATETIME = Field.create("fallback.datetime")
            .withDisplayName("DATETIME zero-date fallback")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Replacement value emitted for MySQL/MariaDB DATETIME zero-date rows " +
                    "(0000-00-00 00:00:00). Use 'NULL' (or leave unset) to emit null and promote the " +
                    "column schema to optional. Otherwise specify a UTC datetime literal " +
                    "(yyyy-MM-dd HH:mm:ss).");

    public static final Field FALLBACK_TIMESTAMP = Field.create("fallback.timestamp")
            .withDisplayName("TIMESTAMP zero-date fallback")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Replacement value emitted for MySQL/MariaDB TIMESTAMP zero-date rows. " +
                    "Use 'NULL' (or leave unset) to emit null and promote the column schema to " +
                    "optional. Otherwise specify an ISO-8601 offset datetime literal " +
                    "(e.g. 1970-01-01T00:00:00Z).");

    public static final Field SELECTOR = Field.create("selector")
            .withDisplayName("Column selector")
            .withType(ConfigDef.Type.STRING)
            .withDefault(DEFAULT_SELECTOR)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Comma-separated list of regex patterns matched against " +
                    "<database>.<table>.<column>. Only matching columns have this converter applied. " +
                    "Default '.*' matches every column whose JDBC type is in target.types.");

    /** Property key prefix for column-level fallback overrides. */
    public static final String COLUMN_FALLBACK_PREFIX = "column.";
    /** Property key suffix for column-level fallback overrides. */
    public static final String COLUMN_FALLBACK_SUFFIX = ".fallback";

    private ZeroDateFallbackConverterConfig() {
    }
}
