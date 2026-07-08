/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Unit tests for the StarRocks {@link ConnectTimeType} handler.
 */
@Tag("UnitTests")
class ConnectTimeTypeTest {

    private static final Schema SCHEMA = SchemaBuilder.int32().name(Time.LOGICAL_NAME).build();

    @Test
    @DisplayName("Should register for Kafka Connect Time logical name")
    void testRegistrationKeys() {
        assertThat(ConnectTimeType.INSTANCE.getRegistrationKeys()).containsExactly(Time.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should format Kafka Connect time values")
    void testBind() {
        // 01:02:03.456 as milliseconds past midnight (UTC)
        final Date value = new Date(((1 * 3600 + 2 * 60 + 3) * 1_000L) + 456);

        final List<ValueBindDescriptor> bindings = ConnectTimeType.INSTANCE.bind(0, SCHEMA, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("01:02:03.456000");
    }
}
