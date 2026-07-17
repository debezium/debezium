/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;

import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.engine.jdbc.Size;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.StructuredDurationType;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should bind structured duration as Db2 string value")
    void shouldBindStructuredDuration() {
        final var schema = StructuredDuration.schema();
        final var value = StructuredDuration.from(schema, 1, 2, 3, 4, 5, 6, 789_000_000);
        final var type = new StructuredDurationType();
        type.configure(mock(SinkConnectorConfig.class), db2Dialect());

        final var bindings = type.bind(4, schema, value);

        assertThat(type.getTypeName(schema, false)).isEqualTo("varchar(128)");
        assertThat(type.getQueryBinding(null, schema, value)).isEqualTo("?");
        assertThat(type.getDefaultValueBinding(schema, value))
                .isEqualTo("'1 years 2 months 3 days 4 hours 5 minutes 6.789 seconds'");
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("1 years 2 months 3 days 4 hours 5 minutes 6.789 seconds");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
    }

    @Test
    @DisplayName("Should bind Db2 timestamp with picosecond precision")
    void shouldBindPicosecondTimestamp() {
        final var schema = StructuredTimestamp.builder(12).build();
        final var value = StructuredTimestamp.fromPicoseconds(schema, 2026, 7, 17, 12, 13, 14, 123_456_789_012L, 12);
        final var type = new StructuredTimestampType();
        type.configure(jdbcConfig(), db2TimestampDialect());
        final var column = timestampColumn(12);

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(12)");
        assertThat(type.getQueryBinding(column, schema, value)).isEqualTo("cast(? as timestamp(12))");
        assertThat(type.getDefaultValueBinding(schema, value)).isEqualTo("'2026-07-17 12:13:14.123456789012'");
        assertThat(type.bind(1, column, schema, value).get(0).getValue()).isEqualTo("2026-07-17 12:13:14.123456789012");
        assertThat(type.bind(1, column, schema, value).get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
    }

    @Test
    @DisplayName("Should reject picosecond loss for a narrower Db2 timestamp")
    void shouldRejectPicosecondLoss() {
        final var schema = StructuredTimestamp.builder(12).build();
        final var value = StructuredTimestamp.fromPicoseconds(schema, 2026, 7, 17, 12, 13, 14, 123_456_789_012L, 12);
        final var type = new StructuredTimestampType();
        type.configure(jdbcConfig(), db2TimestampDialect());

        assertThatThrownBy(() -> type.bind(1, timestampColumn(9), schema, value))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("precision 9");
    }

    private DatabaseDialect db2Dialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getJdbcTypeName(eq(Types.VARCHAR), any(Size.class))).thenReturn("varchar(128)");
        return dialect;
    }

    private JdbcSinkConnectorConfig jdbcConfig() {
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        when(config.useTimeZone()).thenReturn("UTC");
        when(config.getTemporalPrecisionLossHandlingMode()).thenReturn(TemporalPrecisionLossHandlingMode.FAIL);
        return config;
    }

    private DatabaseDialect db2TimestampDialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getMaxTimestampPrecision()).thenReturn(12);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getTargetTemporalCapabilities()).thenReturn(TargetTemporalCapabilities.defaults(6, 12));
        when(dialect.getJdbcTypeName(eq(Types.TIMESTAMP), any(Size.class))).thenReturn("timestamp(12)");
        return dialect;
    }

    private ColumnDescriptor timestampColumn(int precision) {
        return ColumnDescriptor.builder()
                .columnName("ts")
                .jdbcType(Types.TIMESTAMP)
                .typeName("timestamp")
                .scale(precision)
                .build();
    }
}
