/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.Time;

/**
 * Unit tests for the StarRocks {@link TimeType} handler.
 */
@Tag("UnitTests")
class TimeTypeTest {

    private static final Schema SCHEMA = SchemaBuilder.int32().name(Time.SCHEMA_NAME).build();

    @Test
    @DisplayName("Should register for Time logical name")
    void testRegistrationKeys() {
        assertThat(TimeType.INSTANCE.getRegistrationKeys()).containsExactly(Time.SCHEMA_NAME);
    }

    @Test
    @DisplayName("Should map times to varchar as StarRocks has no TIME type")
    void testTypeName() {
        assertThat(TimeType.INSTANCE.getTypeName(SCHEMA, false)).isEqualTo("varchar(18)");
    }

    @Test
    @DisplayName("Should format millisecond time-of-day values")
    void testBind() {
        // 01:02:03.456
        final int millis = ((1 * 3600 + 2 * 60 + 3) * 1_000) + 456;

        final List<ValueBindDescriptor> bindings = TimeType.INSTANCE.bind(0, SCHEMA, millis);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("01:02:03.456000");
    }
}
