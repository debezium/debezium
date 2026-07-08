/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Unit tests for the StarRocks {@link BigIntUnsignedType} handler, including the unsigned
 * 64-bit boundary values.
 */
@Tag("UnitTests")
class BigIntUnsignedTypeTest {

    private static final Schema SCHEMA = SchemaBuilder.int64()
            .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
            .build();

    @Test
    @DisplayName("Should match propagated BIGINT UNSIGNED source column types")
    void testMatchesSourceColumnType() {
        assertThat(BigIntUnsignedType.INSTANCE.matchesSourceColumnType(SCHEMA)).isTrue();
        assertThat(BigIntUnsignedType.INSTANCE.matchesSourceColumnType(Schema.INT64_SCHEMA)).isFalse();
    }

    @Test
    @DisplayName("Should map BIGINT UNSIGNED to largeint to preserve the full unsigned range")
    void testTypeName() {
        assertThat(BigIntUnsignedType.INSTANCE.getTypeName(SCHEMA, false)).isEqualTo("largeint");
    }

    @Test
    @DisplayName("Should restore unsigned values above Long.MAX_VALUE from wrapped longs")
    void testBindWrappedUnsignedBoundary() {
        // 18446744073709551615 (2^64 - 1) arrives as -1 when handled as a long
        assertThat(bind(-1L)).isEqualTo(new BigDecimal("18446744073709551615"));

        // 9223372036854775808 (2^63) arrives as Long.MIN_VALUE
        assertThat(bind(Long.MIN_VALUE)).isEqualTo(new BigDecimal("9223372036854775808"));
    }

    @Test
    @DisplayName("Should pass through values within the signed range")
    void testBindSignedRange() {
        assertThat(bind(Long.MAX_VALUE)).isEqualTo(new BigDecimal("9223372036854775807"));
        assertThat(bind(0L)).isEqualTo(new BigDecimal("0"));
    }

    private static Object bind(long value) {
        final List<ValueBindDescriptor> bindings = BigIntUnsignedType.INSTANCE.bind(0, SCHEMA, value);
        assertThat(bindings).hasSize(1);
        return bindings.get(0).getValue();
    }
}
