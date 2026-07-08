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
import io.debezium.time.MicroTime;

/**
 * Unit tests for the StarRocks {@link MicroTimeType} handler, including the MySQL TIME
 * boundary values of {@code -838:59:59} and {@code 838:59:59}.
 */
@Tag("UnitTests")
class MicroTimeTypeTest {

    private static final Schema SCHEMA = SchemaBuilder.int64().name(MicroTime.SCHEMA_NAME).build();

    @Test
    @DisplayName("Should register for MicroTime logical name")
    void testRegistrationKeys() {
        assertThat(MicroTimeType.INSTANCE.getRegistrationKeys()).containsExactly(MicroTime.SCHEMA_NAME);
    }

    @Test
    @DisplayName("Should map times to varchar as StarRocks has no TIME type")
    void testTypeName() {
        assertThat(MicroTimeType.INSTANCE.getTypeName(SCHEMA, false)).isEqualTo("varchar(18)");
    }

    @Test
    @DisplayName("Should format time-of-day values with microsecond precision")
    void testBindTimeOfDay() {
        // 23:59:59.999999
        final long micros = ((23L * 3600 + 59 * 60 + 59) * 1_000_000) + 999_999;

        assertThat(bind(micros)).isEqualTo("23:59:59.999999");
    }

    @Test
    @DisplayName("Should format the MySQL TIME positive boundary of 838:59:59")
    void testBindPositiveBoundary() {
        final long micros = (838L * 3600 + 59 * 60 + 59) * 1_000_000;

        assertThat(bind(micros)).isEqualTo("838:59:59.000000");
    }

    @Test
    @DisplayName("Should format the MySQL TIME negative boundary of -838:59:59")
    void testBindNegativeBoundary() {
        final long micros = -(838L * 3600 + 59 * 60 + 59) * 1_000_000;

        assertThat(bind(micros)).isEqualTo("-838:59:59.000000");
    }

    @Test
    @DisplayName("Should format midnight")
    void testBindMidnight() {
        assertThat(bind(0L)).isEqualTo("00:00:00.000000");
    }

    @Test
    @DisplayName("Should bind null values")
    void testBindNull() {
        final List<ValueBindDescriptor> bindings = MicroTimeType.INSTANCE.bind(0, SCHEMA, null);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isNull();
    }

    private static String bind(long micros) {
        final List<ValueBindDescriptor> bindings = MicroTimeType.INSTANCE.bind(0, SCHEMA, micros);
        assertThat(bindings).hasSize(1);
        return (String) bindings.get(0).getValue();
    }
}
