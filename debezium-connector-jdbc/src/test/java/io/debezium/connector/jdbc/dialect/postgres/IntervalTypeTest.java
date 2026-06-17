/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.MicroDuration;

/**
 * Unit tests for the PostgreSQL {@link IntervalType} handler.
 */
@Tag("UnitTests")
class IntervalTypeTest {

    private IntervalType intervalType;

    @BeforeEach
    void setUp() {
        intervalType = IntervalType.INSTANCE;
    }

    @Test
    @DisplayName("Should register for MicroDuration logical name")
    void testRegistrationKeys() {
        final String[] keys = intervalType.getRegistrationKeys();
        assertThat(keys).containsExactly(MicroDuration.SCHEMA_NAME);
    }

    @Test
    @DisplayName("Should preserve microseconds when binding interval values")
    void testBindPreservesMicroseconds() {
        assertBindValue(1_123_456L, "1.123456 seconds");
        assertBindValue(45_296_123_456L, "45296.123456 seconds");
        assertBindValue(-1L, "-0.000001 seconds");
        assertBindValue(3_020_398_999_999L, "3020398.999999 seconds");
        assertBindValue(-3_020_398_999_999L, "-3020398.999999 seconds");
        assertBindValue(1_000_000L, "1 seconds");
        assertBindValue(0L, "0 seconds");
    }

    @Test
    @DisplayName("Should preserve microseconds in default value bindings")
    void testDefaultValueBindingPreservesMicroseconds() {
        assertThat(intervalType.getDefaultValueBinding(MicroDuration.schema(), 45_296_123_456L))
                .isEqualTo("'45296.123456 seconds'");
        assertThat(intervalType.getDefaultValueBinding(MicroDuration.schema(), -1L))
                .isEqualTo("'-0.000001 seconds'");
    }

    @Test
    @DisplayName("Should use interval cast query binding")
    void testQueryBinding() {
        assertThat(intervalType.getQueryBinding(null, null, null)).isEqualTo("cast(? as interval)");
    }

    private void assertBindValue(long durationMicros, String expectedValue) {
        final List<ValueBindDescriptor> bindings = intervalType.bind(1, MicroDuration.schema(), durationMicros);
        assertThat(bindings).hasSize(1);

        final ValueBindDescriptor binding = bindings.get(0);
        assertThat(binding.getIndex()).isEqualTo(1);
        assertThat(binding.getValue()).isEqualTo(expectedValue);
        assertThat(binding.getTargetSqlType()).isNull();
    }
}
