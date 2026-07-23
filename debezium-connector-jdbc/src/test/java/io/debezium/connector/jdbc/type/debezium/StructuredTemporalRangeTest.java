/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalRangeLossHandlingMode;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities.ZonedTimestampRangeBasis;
import io.debezium.connector.jdbc.type.debezium.TemporalRange.Boundary;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredTimestamp;

@Tag("UnitTests")
class StructuredTemporalRangeTest {

    private static final TemporalRange STANDARD_TIMESTAMP_RANGE = TemporalRange.timestampYears(1, 9999);

    @Test
    @DisplayName("Should reject structured timestamps outside the target range by default")
    void shouldRejectOutOfRangeTimestamp() {
        final var schema = StructuredTimestamp.builder(6).build();
        final var value = StructuredTimestamp.from(schema, 12_000, 1, 1, 12, 13, 14, 123_456_000, 6);

        assertThatThrownBy(() -> StructuredTemporalSupport.adjustTimestamp(
                value, 6, TemporalPrecisionLossHandlingMode.FAIL, STANDARD_TIMESTAMP_RANGE,
                TemporalRangeLossHandlingMode.FAIL, "target column 'ts' (timestamp)"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("12000-01-01")
                .hasMessageContaining("temporal.range.loss.handling.mode")
                .hasMessageContaining("target column 'ts'");
    }

    @Test
    @DisplayName("Should saturate after fractional precision reduction")
    void shouldSaturateAfterPrecisionReduction() {
        final var schema = StructuredTimestamp.builder(12).build();
        final var value = StructuredTimestamp.fromPicoseconds(
                schema, 9999, 12, 31, 23, 59, 59, 999_999_500_000L, 12);

        final Boundary adjusted = StructuredTemporalSupport.adjustTimestamp(
                value, 6, TemporalPrecisionLossHandlingMode.ROUND, STANDARD_TIMESTAMP_RANGE,
                TemporalRangeLossHandlingMode.SATURATE, "target type 'timestamp(6)'");

        assertThat(adjusted).isEqualTo(Boundary.timestamp(
                9999, 12, 31, 23, 59, 59, 999_999_000_000L));
    }

    @Test
    @DisplayName("Should apply range boundaries truncated to the target precision")
    void shouldApplyRangeAtTargetPrecision() {
        final var schema = StructuredTimestamp.builder(6).build();
        final var maxAtMicros = StructuredTimestamp.from(schema, 9999, 12, 31, 23, 59, 59, 999_999_000, 6);

        assertThat(StructuredTemporalSupport.adjustTimestamp(
                maxAtMicros, 6, TemporalPrecisionLossHandlingMode.FAIL, STANDARD_TIMESTAMP_RANGE,
                TemporalRangeLossHandlingMode.FAIL, "target type 'datetime(6)'"))
                .isEqualTo(Boundary.timestamp(9999, 12, 31, 23, 59, 59, 999_999_000_000L));

        assertThat(StructuredTemporalSupport.adjustTimestamp(
                StructuredTimestamp.from(schema, 9999, 12, 31, 23, 59, 59, 0, 6), 0,
                TemporalPrecisionLossHandlingMode.ROUND, STANDARD_TIMESTAMP_RANGE,
                TemporalRangeLossHandlingMode.FAIL, "target type 'datetime'"))
                .isEqualTo(Boundary.timestamp(9999, 12, 31, 23, 59, 59, 0));

        // rounding the fraction up at precision 0 overflows past the truncated maximum
        assertThatThrownBy(() -> StructuredTemporalSupport.adjustTimestamp(
                maxAtMicros, 0, TemporalPrecisionLossHandlingMode.ROUND, STANDARD_TIMESTAMP_RANGE,
                TemporalRangeLossHandlingMode.FAIL, "target type 'datetime'"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("10000-01-01 00:00:00")
                .hasMessageContaining("23:59:59]");
    }

    @Test
    @DisplayName("Should saturate finite dates and infinity to the nearest boundary")
    void shouldSaturateDateAndInfinity() {
        final TemporalRange dateRange = TemporalRange.dateYears(1, 9999);
        final var dateSchema = StructuredDate.schema();
        final var timestampSchema = StructuredTimestamp.schema();

        assertThat(StructuredTemporalSupport.adjustDate(
                StructuredDate.from(dateSchema, 0, 12, 31), dateRange,
                TemporalRangeLossHandlingMode.SATURATE, "target date")).isEqualTo(dateRange.minimum());
        assertThat(StructuredTemporalSupport.adjustTimestamp(
                StructuredTimestamp.positiveInfinity(timestampSchema), 6, TemporalPrecisionLossHandlingMode.FAIL,
                STANDARD_TIMESTAMP_RANGE, TemporalRangeLossHandlingMode.SATURATE, "target timestamp"))
                .isEqualTo(STANDARD_TIMESTAMP_RANGE.maximum().withPrecision(6));
    }

    @Test
    @DisplayName("Should resolve actual column ranges and zoned timestamp range basis")
    void shouldResolveActualTargetRange() {
        final TemporalRange narrowRange = new TemporalRange(
                Boundary.timestamp(1970, 1, 1, 0, 0, 1, 0),
                Boundary.timestamp(2038, 1, 19, 3, 14, 7, 0));
        final var capabilities = TargetTemporalCapabilities.defaults(6, 6)
                .withTimestampRange(STANDARD_TIMESTAMP_RANGE)
                .withTimestampRangeForType(narrowRange, "timestamp")
                .withZonedTimestampRangeBasis(ZonedTimestampRangeBasis.LOCAL_AND_INSTANT);
        final var column = ColumnDescriptor.builder()
                .columnName("ts")
                .jdbcType(Types.TIMESTAMP)
                .typeName("TIMESTAMP(6)")
                .scale(6)
                .build();

        assertThat(capabilities.targetTimestampRange(column)).isEqualTo(narrowRange);
        assertThat(capabilities.targetZonedTimestampRange(null, 14 * 3_600).minimum())
                .isEqualTo(Boundary.timestamp(1, 1, 1, 14, 0, 0, 0));
        assertThat(capabilities.targetZonedTimestampRange(null, 14 * 3_600).maximum())
                .isEqualTo(STANDARD_TIMESTAMP_RANGE.maximum());
    }
}
