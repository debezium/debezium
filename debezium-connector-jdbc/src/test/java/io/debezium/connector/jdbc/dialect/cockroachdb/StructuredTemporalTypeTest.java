/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.time.LocalDateTime;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalRangeLossHandlingMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.postgres.StructuredDateType;
import io.debezium.connector.jdbc.dialect.postgres.StructuredTimestampType;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities;
import io.debezium.connector.jdbc.type.debezium.TemporalRange.Boundary;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should declare CockroachDB timestamp bounds without timestamp infinity support")
    void shouldDeclareCockroachDbTemporalCapabilities() {
        final var capabilities = cockroachCapabilities();

        assertThat(capabilities.dateInfinitySupported()).isTrue();
        assertThat(capabilities.timestampInfinitySupported()).isFalse();
        assertThat(capabilities.timestampRange().minimum())
                .isEqualTo(Boundary.timestamp(-4714, 11, 24, 0, 0, 0, 0));
        assertThat(capabilities.timestampRange().maximum())
                .isEqualTo(Boundary.timestamp(294_276, 12, 31, 23, 59, 59, 999_999_000_000L));
    }

    @Test
    @DisplayName("Should reject CockroachDB timestamp infinity in fail mode")
    void shouldRejectTimestampInfinity() {
        final var schema = StructuredTimestamp.schema();
        final var type = configuredTimestampType(TemporalRangeLossHandlingMode.FAIL);

        assertThatThrownBy(() -> type.bind(1, timestampColumn(), schema, StructuredTimestamp.positiveInfinity(schema)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Non-finite structured temporal value")
                .hasMessageContaining("ts");
    }

    @Test
    @DisplayName("Should saturate CockroachDB timestamp infinity to its finite boundary")
    void shouldSaturateTimestampInfinity() {
        final var schema = StructuredTimestamp.schema();
        final var type = configuredTimestampType(TemporalRangeLossHandlingMode.SATURATE);

        assertThat(type.bind(1, timestampColumn(), schema, StructuredTimestamp.positiveInfinity(schema)).get(0).getValue())
                .isEqualTo(LocalDateTime.of(294_276, 12, 31, 23, 59, 59, 999_999_000));
    }

    @Test
    @DisplayName("Should retain CockroachDB date infinity")
    void shouldRetainDateInfinity() {
        final var schema = StructuredDate.schema();
        final var type = configuredDateType();

        assertThat(type.bind(1, dateColumn(), schema, StructuredDate.positiveInfinity(schema)).get(0).getValue())
                .isEqualTo("infinity");
    }

    private StructuredTimestampType configuredTimestampType(TemporalRangeLossHandlingMode rangeMode) {
        final var type = new StructuredTimestampType();
        type.configure(config(rangeMode), dialect());
        return type;
    }

    private StructuredDateType configuredDateType() {
        final var type = new StructuredDateType();
        type.configure(config(TemporalRangeLossHandlingMode.FAIL), dialect());
        return type;
    }

    private JdbcSinkConnectorConfig config(TemporalRangeLossHandlingMode rangeMode) {
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        when(config.useTimeZone()).thenReturn("UTC");
        when(config.getTemporalPrecisionLossHandlingMode()).thenReturn(TemporalPrecisionLossHandlingMode.FAIL);
        when(config.getTemporalRangeLossHandlingMode()).thenReturn(rangeMode);
        return config;
    }

    private DatabaseDialect dialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        final TargetTemporalCapabilities capabilities = cockroachCapabilities();
        when(dialect.getMaxTimestampPrecision()).thenReturn(6);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getTargetTemporalCapabilities()).thenReturn(capabilities);
        return dialect;
    }

    private TargetTemporalCapabilities cockroachCapabilities() {
        final CockroachDBDatabaseDialect dialect = mock(CockroachDBDatabaseDialect.class, CALLS_REAL_METHODS);
        doReturn(6).when(dialect).getMaxTimePrecision();
        doReturn(6).when(dialect).getMaxTimestampPrecision();
        return dialect.getTargetTemporalCapabilities();
    }

    private ColumnDescriptor timestampColumn() {
        return ColumnDescriptor.builder()
                .columnName("ts")
                .jdbcType(Types.TIMESTAMP)
                .typeName("timestamp")
                .scale(6)
                .build();
    }

    private ColumnDescriptor dateColumn() {
        return ColumnDescriptor.builder()
                .columnName("d")
                .jdbcType(Types.DATE)
                .typeName("date")
                .build();
    }
}
