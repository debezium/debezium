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
import io.debezium.time.NanoTime;

/**
 * Unit tests for the StarRocks {@link NanoTimeType} handler.
 */
@Tag("UnitTests")
class NanoTimeTypeTest {

    private static final Schema SCHEMA = SchemaBuilder.int64().name(NanoTime.SCHEMA_NAME).build();

    @Test
    @DisplayName("Should register for NanoTime logical name")
    void testRegistrationKeys() {
        assertThat(NanoTimeType.INSTANCE.getRegistrationKeys()).containsExactly(NanoTime.SCHEMA_NAME);
    }

    @Test
    @DisplayName("Should truncate nanoseconds to microsecond precision")
    void testBindTruncatesNanoseconds() {
        // 12:34:56.123456789
        final long nanos = ((12L * 3600 + 34 * 60 + 56) * 1_000_000_000L) + 123_456_789L;

        final List<ValueBindDescriptor> bindings = NanoTimeType.INSTANCE.bind(0, SCHEMA, nanos);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("12:34:56.123456");
    }
}
