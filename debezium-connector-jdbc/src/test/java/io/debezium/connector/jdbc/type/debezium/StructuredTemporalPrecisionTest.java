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
    @DisplayName("Should keep default precision for non-structured timestamp zero precision")
    void shouldKeepDefaultPrecisionForNonStructuredTimestampZeroPrecision() {
        final var type = new io.debezium.connector.jdbc.type.connect.ConnectTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = org.apache.kafka.connect.data.Timestamp.builder()
                .parameter("__debezium.source.column.scale", "0")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(6)");
    }

    @Test
    @DisplayName("Should keep default precision for non-structured time zero precision")
    void shouldKeepDefaultPrecisionForNonStructuredTimeZeroPrecision() {
        final var type = new io.debezium.connector.jdbc.type.connect.ConnectTimeType();
        type.configure(config(), timeDialect());
        final var schema = org.apache.kafka.connect.data.Time.builder()
                .parameter("__debezium.source.column.scale", "0")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("time(6)");
    }

    @Test
    @DisplayName("Should use propagated source column scale for structured timestamp precision")
    void shouldUseSourceColumnScaleForStructuredTimestampPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder()
                .parameter("__debezium.source.column.scale", "6")
                .parameter("__debezium.source.column.length", "6")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(6)");
    }

    @Test
    @DisplayName("Should preserve source column zero precision")
    void shouldPreserveSourceColumnZeroPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder()
                .parameter("__debezium.source.column.scale", "0")
                .build();

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
    @DisplayName("Should use propagated source column scale for structured time precision")
    void shouldUseSourceColumnScaleForStructuredTimePrecision() {
        final var type = new StructuredTimeType();
        type.configure(config(), timeDialect());
        final var schema = StructuredTime.builder()
                .parameter("__debezium.source.column.scale", "6")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("time(6)");
    }

    @Test
    @DisplayName("Should use propagated source column scale for structured zoned timestamp precision")
    void shouldUseSourceColumnScaleForStructuredZonedTimestampPrecision() {
        final var type = new StructuredZonedTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredZonedTimestamp.builder()
                .parameter("__debezium.source.column.scale", "3")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamptz(3)");
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
