/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;

import org.hibernate.engine.jdbc.Size;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.time.StructuredTime;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;

@Tag("UnitTests")
class StructuredTemporalPrecisionTest {

    @Test
    @DisplayName("Should prefer structured timestamp precision over propagated source column precision")
    void shouldPreferStructuredTimestampPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder(3)
                .parameter("__debezium.source.column.scale", "6")
                .parameter("__debezium.source.column.length", "6")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(3)");
    }

    @Test
    @DisplayName("Should preserve structured timestamp zero precision")
    void shouldPreserveStructuredTimestampZeroPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder(0).build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(0)");
    }

    @Test
    @DisplayName("Should fallback to propagated source column precision")
    void shouldFallbackToSourceColumnPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder()
                .parameter("__debezium.source.column.scale", "4")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(4)");
    }

    @Test
    @DisplayName("Should prefer structured time precision over propagated source column precision")
    void shouldPreferStructuredTimePrecision() {
        final var type = new StructuredTimeType();
        type.configure(config(), timeDialect());
        final var schema = StructuredTime.builder(0)
                .parameter("__debezium.source.column.scale", "6")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("time(0)");
    }

    @Test
    @DisplayName("Should prefer structured zoned timestamp precision")
    void shouldPreferStructuredZonedTimestampPrecision() {
        final var type = new StructuredZonedTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredZonedTimestamp.builder(7)
                .parameter("__debezium.source.column.scale", "3")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamptz(7)");
    }

    private SinkConnectorConfig config() {
        final SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.useTimeZone()).thenReturn("UTC");
        return config;
    }

    private DatabaseDialect timestampDialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getMaxTimestampPrecision()).thenReturn(9);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getJdbcTypeName(eq(Types.TIMESTAMP), any(Size.class)))
                .thenAnswer(invocation -> "timestamp(" + size(invocation.getArgument(1)) + ")");
        when(dialect.getJdbcTypeName(eq(Types.TIMESTAMP_WITH_TIMEZONE), any(Size.class)))
                .thenAnswer(invocation -> "timestamptz(" + size(invocation.getArgument(1)) + ")");
        return dialect;
    }

    private DatabaseDialect timeDialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getDefaultTimePrecision()).thenReturn(6);
        when(dialect.getJdbcTypeName(eq(Types.TIME), any(Size.class)))
                .thenAnswer(invocation -> "time(" + size(invocation.getArgument(1)) + ")");
        return dialect;
    }

    private Integer size(Size size) {
        return size.getPrecision();
    }
}
