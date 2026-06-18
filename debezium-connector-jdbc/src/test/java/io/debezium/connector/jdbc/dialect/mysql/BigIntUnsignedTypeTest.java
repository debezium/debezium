/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the MySQL {@link BigIntUnsignedType} handler.
 */
@Tag("UnitTests")
class BigIntUnsignedTypeTest {

    private static final String UNSIGNED_LONG_MAX_VALUE = "18446744073709551615";

    private final BigIntUnsignedType type = BigIntUnsignedType.INSTANCE;

    @Test
    @DisplayName("Should register for MySQL unsigned bigint source column types")
    void shouldRegisterForUnsignedBigIntSourceColumnTypes() {
        assertThat(type.getRegistrationKeys()).containsExactly(
                "BIGINT UNSIGNED",
                "BIGINT UNSIGNED ZEROFILL",
                "INT8 UNSIGNED",
                "INT8 UNSIGNED ZEROFILL");
    }

    @Test
    @DisplayName("Should resolve to MySQL unsigned bigint")
    void shouldResolveToUnsignedBigInt() {
        final Schema schema = SchemaBuilder.int64()
                .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("bigint unsigned");
        assertThat(type.getTypeName(schema, true)).isEqualTo("bigint unsigned");
    }

    @Test
    @DisplayName("Should match MySQL unsigned bigint source column types")
    void shouldMatchUnsignedBigIntSourceColumnTypes() {
        final Schema schema = SchemaBuilder.int64()
                .parameter("__debezium.source.column.type", "bigint unsigned")
                .build();

        assertThat(type.matchesSourceColumnType(schema)).isTrue();
    }

    @Test
    @DisplayName("Should not match unrelated source column types")
    void shouldNotMatchUnrelatedSourceColumnTypes() {
        final Schema schema = SchemaBuilder.bytes()
                .name(org.apache.kafka.connect.data.Decimal.LOGICAL_NAME)
                .parameter("__debezium.source.column.type", "money")
                .parameter("scale", "4")
                .build();

        assertThat(type.matchesSourceColumnType(schema)).isFalse();
    }

    @Test
    @DisplayName("Should bind negative long values as unsigned bigint values")
    void shouldBindNegativeLongAsUnsignedBigInt() {
        final Schema schema = SchemaBuilder.int64()
                .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
                .build();

        final var bindings = type.bind(1, schema, -1L);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getIndex()).isEqualTo(1);
        assertThat(bindings.get(0).getValue()).isEqualTo(new BigDecimal(UNSIGNED_LONG_MAX_VALUE));
    }

    @Test
    @DisplayName("Should bind decimal values unchanged")
    void shouldBindDecimalValuesUnchanged() {
        final Schema schema = SchemaBuilder.bytes()
                .name(org.apache.kafka.connect.data.Decimal.LOGICAL_NAME)
                .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
                .parameter("scale", "0")
                .build();
        final var value = new BigDecimal(UNSIGNED_LONG_MAX_VALUE);

        final var bindings = type.bind(1, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo(value);
    }

    @Test
    @DisplayName("Should bind negative long default values as unsigned bigint values")
    void shouldBindNegativeLongDefaultsAsUnsignedBigInt() {
        final Schema schema = SchemaBuilder.int64()
                .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
                .build();

        assertThat(type.getDefaultValueBinding(schema, -1L)).isEqualTo(UNSIGNED_LONG_MAX_VALUE);
    }
}
