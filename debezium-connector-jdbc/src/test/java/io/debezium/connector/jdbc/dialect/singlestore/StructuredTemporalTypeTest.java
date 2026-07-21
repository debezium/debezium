/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalRangeLossHandlingMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.mysql.StructuredDateType;
import io.debezium.connector.jdbc.dialect.mysql.StructuredTimestampType;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities;
import io.debezium.connector.jdbc.type.debezium.TemporalRange.Boundary;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should declare SingleStore date and timestamp ranges")
    void shouldDeclareSingleStoreTemporalCapabilities() {
        final var capabilities = singleStoreCapabilities();

        assertThat(capabilities.zeroDateSupported()).isFalse();
        assertThat(capabilities.dateRange().minimum()).isEqualTo(Boundary.date(1000, 1, 1));
        assertThat(capabilities.timestampRange().maximum())
                .isEqualTo(Boundary.timestamp(9999, 12, 31, 23, 59, 59, 999_999_000_000L));
        assertThat(capabilities.targetTimestampRange(timestampColumn("timestamp")).maximum())
                .isEqualTo(Boundary.timestamp(2038, 1, 19, 3, 14, 7, 999_999_000_000L));
    }

    @Test
    @DisplayName("Should reject SingleStore zero dates in fail mode")
    void shouldRejectZeroDate() {
        final var schema = StructuredDate.schema();
        final var type = configuredDateType(TemporalRangeLossHandlingMode.FAIL);

        assertThatThrownBy(() -> type.bind(1, dateColumn(), schema, StructuredDate.from(schema, 0, 0, 0)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("0000-00-00")
                .hasMessageContaining("saturate");
    }

    @Test
    @DisplayName("Should saturate SingleStore zero dates to the minimum finite date")
    void shouldSaturateZeroDate() {
        final var schema = StructuredDate.schema();
        final var type = configuredDateType(TemporalRangeLossHandlingMode.SATURATE);

        assertThat(type.bind(1, dateColumn(), schema, StructuredDate.from(schema, 0, 0, 0)).get(0).getValue())
                .isEqualTo("1000-01-01");
    }

    @Test
    @DisplayName("Should retain the SingleStore datetime microsecond upper boundary")
    void shouldRetainDatetimeUpperBoundary() {
        final var schema = StructuredTimestamp.builder(6).build();
        final var value = StructuredTimestamp.from(
                schema, 9999, 12, 31, 23, 59, 59, 999_999_000, 6);
        final var type = configuredTimestampType(TemporalRangeLossHandlingMode.FAIL);

        assertThat(type.bind(1, timestampColumn("datetime"), schema, value).get(0).getValue())
                .isEqualTo("9999-12-31 23:59:59.999999");
    }

    @Test
    @DisplayName("Should derive SingleStore time precision from column size metadata")
    void shouldDeriveTimePrecisionFromColumnSize() {
        final var schema = StructuredDuration.builder(6, StructuredDuration.Kind.ELAPSED_TIME).build();
        final var value = StructuredDuration.from(schema, 0, 0, 0, 1, 2, 3, 123_456_000, 6);
        final var type = new SingleStoreStructuredDurationType();
        type.configure(config(TemporalRangeLossHandlingMode.FAIL), dialect());

        assertThat(type.bind(1, timeColumn(17), schema, value).get(0).getValue())
                .isEqualTo("001:02:03.123456");
        assertThatThrownBy(() -> type.bind(1, timeColumn(10), schema, value))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("precision 0");
    }

    private StructuredDateType configuredDateType(TemporalRangeLossHandlingMode rangeMode) {
        final var type = new StructuredDateType();
        type.configure(config(rangeMode), dialect());
        return type;
    }

    private StructuredTimestampType configuredTimestampType(TemporalRangeLossHandlingMode rangeMode) {
        final var type = new StructuredTimestampType();
        type.configure(config(rangeMode), dialect());
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
        final TargetTemporalCapabilities capabilities = singleStoreCapabilities();
        when(dialect.getMaxTimestampPrecision()).thenReturn(6);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getTargetTemporalCapabilities()).thenReturn(capabilities);
        return dialect;
    }

    private TargetTemporalCapabilities singleStoreCapabilities() {
        final SingleStoreDatabaseDialect dialect = mock(SingleStoreDatabaseDialect.class, CALLS_REAL_METHODS);
        doReturn(6).when(dialect).getMaxTimePrecision();
        doReturn(6).when(dialect).getMaxTimestampPrecision();
        return dialect.getTargetTemporalCapabilities();
    }

    private ColumnDescriptor timestampColumn(String typeName) {
        return ColumnDescriptor.builder()
                .columnName("ts")
                .jdbcType(Types.TIMESTAMP)
                .typeName(typeName)
                .precision(26)
                .scale(0)
                .build();
    }

    private ColumnDescriptor timeColumn(int columnSize) {
        return ColumnDescriptor.builder()
                .columnName("t")
                .jdbcType(Types.TIME)
                .typeName("time")
                .precision(columnSize)
                .scale(0)
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
