/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.sql.Types;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.time.StructuredTime;
import io.debezium.time.StructuredZonedTime;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should preserve picosecond structured times as StarRocks strings")
    void shouldBindStructuredTimeAsString() {
        final var schema = StructuredTime.builder(12).build();
        final var value = StructuredTime.fromPicoseconds(schema, 12, 13, 14, 123_456_789_012L, 12);

        assertThat(StructuredTimeType.INSTANCE.getTypeName(schema, false)).isEqualTo("varchar(21)");
        assertThat(StructuredTimeType.INSTANCE.bind(1, schema, value).get(0))
                .satisfies(binding -> {
                    assertThat(binding.getValue()).isEqualTo("12:13:14.123456789012");
                    assertThat(binding.getTargetSqlType()).isEqualTo(Types.VARCHAR);
                });
        assertThat(StructuredTimeType.INSTANCE.getDefaultValueBinding(schema, value))
                .isEqualTo("'12:13:14.123456789012'");
    }

    @Test
    @DisplayName("Should preserve picoseconds and offsets for StarRocks structured zoned times")
    void shouldBindStructuredZonedTimeAsString() {
        final var schema = StructuredZonedTime.builder(12).build();
        final var value = StructuredZonedTime.fromPicoseconds(
                schema, 12, 13, 14, 123_456_789_012L, 9 * 3_600, 12);

        assertThat(StructuredZonedTimeType.INSTANCE.getTypeName(schema, false)).isEqualTo("varchar(32)");
        assertThat(StructuredZonedTimeType.INSTANCE.bind(1, schema, value).get(0))
                .satisfies(binding -> {
                    assertThat(binding.getValue()).isEqualTo("12:13:14.123456789012+09:00");
                    assertThat(binding.getTargetSqlType()).isEqualTo(Types.VARCHAR);
                });
    }

    @Test
    @DisplayName("Should declare picosecond time storage capability for StarRocks string types")
    void shouldDeclareStarRocksTimeCapability() {
        final StarRocksDatabaseDialect dialect = mock(StarRocksDatabaseDialect.class, CALLS_REAL_METHODS);
        doReturn(6).when(dialect).getMaxTimestampPrecision();

        assertThat(dialect.getTargetTemporalCapabilities().maxTimePrecision()).isEqualTo(12);
    }
}
