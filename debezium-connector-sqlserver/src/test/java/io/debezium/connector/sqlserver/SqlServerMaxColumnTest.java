/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Types;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Tests for max-type column handling (varchar(max), nvarchar(max), varbinary(max))
 * and the unavailable value placeholder mechanism.
 */
public class SqlServerMaxColumnTest {

    private static final byte[] PLACEHOLDER = RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER
            .getBytes(StandardCharsets.UTF_8);

    @Test
    @FixFor("dbz#1164")
    void shouldIdentifyMaxColumnJdbcTypes() {
        assertThat(SqlServerDatabaseSchema.isMaxColumnJdbcType(Types.LONGVARCHAR)).isTrue();
        assertThat(SqlServerDatabaseSchema.isMaxColumnJdbcType(Types.LONGNVARCHAR)).isTrue();
        assertThat(SqlServerDatabaseSchema.isMaxColumnJdbcType(Types.LONGVARBINARY)).isTrue();
        assertThat(SqlServerDatabaseSchema.isMaxColumnJdbcType(Types.VARCHAR)).isFalse();
        assertThat(SqlServerDatabaseSchema.isMaxColumnJdbcType(Types.NVARCHAR)).isFalse();
        assertThat(SqlServerDatabaseSchema.isMaxColumnJdbcType(Types.VARBINARY)).isFalse();
        assertThat(SqlServerDatabaseSchema.isMaxColumnJdbcType(Types.INTEGER)).isFalse();
    }

    @Test
    @FixFor("dbz#1164")
    void shouldIdentifyMaxColumnFromColumnModel() {
        final var varcharMax = Column.editor()
                .name("col_varchar_max")
                .jdbcType(Types.LONGVARCHAR)
                .create();
        final var nvarcharMax = Column.editor()
                .name("col_nvarchar_max")
                .jdbcType(Types.LONGNVARCHAR)
                .create();
        final var varbinaryMax = Column.editor()
                .name("col_varbinary_max")
                .jdbcType(Types.LONGVARBINARY)
                .create();
        final var varchar100 = Column.editor()
                .name("col_varchar")
                .jdbcType(Types.VARCHAR)
                .create();
        final var intCol = Column.editor()
                .name("col_int")
                .jdbcType(Types.INTEGER)
                .create();

        assertThat(SqlServerDatabaseSchema.isMaxColumn(varcharMax)).isTrue();
        assertThat(SqlServerDatabaseSchema.isMaxColumn(nvarcharMax)).isTrue();
        assertThat(SqlServerDatabaseSchema.isMaxColumn(varbinaryMax)).isTrue();
        assertThat(SqlServerDatabaseSchema.isMaxColumn(varchar100)).isFalse();
        assertThat(SqlServerDatabaseSchema.isMaxColumn(intCol)).isFalse();
    }

    @Test
    @FixFor("dbz#1164")
    void shouldConvertUnavailableValueToPlaceholderString() {
        final var converters = new SqlServerValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                PLACEHOLDER);

        final var column = Column.editor()
                .name("col_varchar_max")
                .jdbcType(Types.LONGVARCHAR)
                .optional(true)
                .create();

        final var fieldDefn = new org.apache.kafka.connect.data.Field(
                "col_varchar_max", 0, SchemaBuilder.string().optional().build());

        final var converter = converters.converter(column, fieldDefn);

        // UNAVAILABLE_VALUE should be converted to placeholder string
        final var result = converter.convert(SqlServerValueConverters.UNAVAILABLE_VALUE);
        assertThat(result).isEqualTo(RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER);

        // null should remain null for optional columns
        final var nullResult = converter.convert(null);
        assertThat(nullResult).isNull();

        // Regular string values should pass through
        final var regularResult = converter.convert("hello");
        assertThat(regularResult).isEqualTo("hello");
    }

    @Test
    @FixFor("dbz#1164")
    void shouldConvertUnavailableValueToPlaceholderBinary() {
        final var converters = new SqlServerValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                PLACEHOLDER);

        final var column = Column.editor()
                .name("col_varbinary_max")
                .jdbcType(Types.LONGVARBINARY)
                .optional(true)
                .create();

        final var fieldDefn = new org.apache.kafka.connect.data.Field(
                "col_varbinary_max", 0, Schema.OPTIONAL_BYTES_SCHEMA);

        final var converter = converters.converter(column, fieldDefn);

        // UNAVAILABLE_VALUE should be converted to placeholder bytes
        final var result = converter.convert(SqlServerValueConverters.UNAVAILABLE_VALUE);
        assertThat(result).isNotNull();

        // null should remain null for optional columns
        final var nullResult = converter.convert(null);
        assertThat(nullResult).isNull();
    }

    @Test
    @FixFor("dbz#1164")
    void shouldReturnPlaceholderValues() {
        final var converters = new SqlServerValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                PLACEHOLDER);

        assertThat(converters.getUnavailableValuePlaceholderString())
                .isEqualTo(RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER);
        assertThat(converters.getUnavailableValuePlaceholderBinary())
                .isEqualTo(PLACEHOLDER);
    }

    @Test
    @FixFor("dbz#1164")
    void shouldReturnNullPlaceholderWhenNotConfigured() {
        final var converters = new SqlServerValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES);

        assertThat(converters.getUnavailableValuePlaceholderString()).isNull();
        assertThat(converters.getUnavailableValuePlaceholderBinary()).isNull();
    }
}
