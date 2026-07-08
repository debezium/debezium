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
import io.debezium.time.ZonedTime;

/**
 * Unit tests for the StarRocks {@link ZonedTimeType} handler.
 */
@Tag("UnitTests")
class ZonedTimeTypeTest {

    private static final Schema SCHEMA = SchemaBuilder.string().name(ZonedTime.SCHEMA_NAME).build();

    @Test
    @DisplayName("Should register for ZonedTime logical name")
    void testRegistrationKeys() {
        assertThat(ZonedTimeType.INSTANCE.getRegistrationKeys()).containsExactly(ZonedTime.SCHEMA_NAME);
    }

    @Test
    @DisplayName("Should map zoned times to varchar as StarRocks has no time zone aware types")
    void testTypeName() {
        assertThat(ZonedTimeType.INSTANCE.getTypeName(SCHEMA, false)).isEqualTo("varchar(32)");
    }

    @Test
    @DisplayName("Should store the ISO-8601 offset time representation verbatim")
    void testBind() {
        final List<ValueBindDescriptor> bindings = ZonedTimeType.INSTANCE.bind(0, SCHEMA, "10:15:30.123456+09:00");

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("10:15:30.123456+09:00");
    }
}
