/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoTime;
import io.debezium.time.Time;
import io.debezium.time.ZonedTime;

/**
 * Unit tests for PostgreSQL time boundary values.
 */
@Tag("UnitTests")
class TimeTypeTest {

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should bind Debezium Time boundary value as PostgreSQL time literal")
    void testBindTimeBoundaryValue() {
        assertTimeBoundary(TimeType.INSTANCE, Time.schema(), (int) TimeUnit.DAYS.toMillis(1));
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should bind Debezium MicroTime boundary value as PostgreSQL time literal")
    void testBindMicroTimeBoundaryValue() {
        assertTimeBoundary(MicroTimeType.INSTANCE, MicroTime.schema(), TimeUnit.DAYS.toMicros(1));
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should bind Debezium NanoTime boundary value as PostgreSQL time literal")
    void testBindNanoTimeBoundaryValue() {
        assertTimeBoundary(NanoTimeType.INSTANCE, NanoTime.schema(), TimeUnit.DAYS.toNanos(1));
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should bind Debezium ZonedTime boundary value as PostgreSQL time with time zone literal")
    void testBindTimeWithTimezoneBoundaryValue() {
        final List<ValueBindDescriptor> descriptors = TimeWithTimezoneType.INSTANCE.bind(1, ZonedTime.schema(), "24:00:00Z");

        assertThat(TimeWithTimezoneType.INSTANCE.getQueryBinding(null, ZonedTime.schema(), "24:00:00Z")).isEqualTo("cast(? as time with time zone)");
        assertThat(descriptors).hasSize(1);
        assertThat(descriptors.get(0).getIndex()).isEqualTo(1);
        assertThat(descriptors.get(0).getValue()).isEqualTo("24:00:00Z");
        assertThat(descriptors.get(0).getTargetSqlType()).isNull();
    }

    private void assertTimeBoundary(io.debezium.connector.jdbc.type.JdbcType type, Schema schema, Number value) {
        final List<ValueBindDescriptor> descriptors = type.bind(1, schema, value);

        assertThat(type.getQueryBinding(null, schema, value)).isEqualTo("cast(? as time)");
        assertThat(descriptors).hasSize(1);
        assertThat(descriptors.get(0).getIndex()).isEqualTo(1);
        assertThat(descriptors.get(0).getValue()).isEqualTo("24:00:00");
        assertThat(descriptors.get(0).getTargetSqlType()).isNull();
    }
}
