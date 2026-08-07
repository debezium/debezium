/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;

class CudEventFactoryTemporalTest {

    @Test
    @DisplayName("Converts io.debezium.time.Date (days since epoch) to an ISO date string")
    void convertsDate() {
        // 2024-01-01 is 19723 days after 1970-01-01
        assertThat(CudEventFactory.normalizeTemporal(19723, Date.schema())).isEqualTo("2024-01-01");
    }

    @Test
    @DisplayName("Converts io.debezium.time.Timestamp (epoch millis) to a UTC ISO datetime with millisecond precision")
    void convertsTimestamp() {
        assertThat(CudEventFactory.normalizeTemporal(1704112245123L, Timestamp.schema()))
                .isEqualTo("2024-01-01T12:30:45.123Z");
    }

    @Test
    @DisplayName("Converts io.debezium.time.MicroTimestamp (epoch micros) to a UTC ISO datetime with microsecond precision")
    void convertsMicroTimestamp() {
        assertThat(CudEventFactory.normalizeTemporal(1704112245123456L, MicroTimestamp.schema()))
                .isEqualTo("2024-01-01T12:30:45.123456Z");
    }

    @Test
    @DisplayName("Converts io.debezium.time.NanoTimestamp (epoch nanos) to a UTC ISO datetime with nanosecond precision")
    void convertsNanoTimestamp() {
        assertThat(CudEventFactory.normalizeTemporal(1704112245123456789L, NanoTimestamp.schema()))
                .isEqualTo("2024-01-01T12:30:45.123456789Z");
    }

    @Test
    @DisplayName("Converts io.debezium.time.Time (millis since midnight) to a UTC ISO time with millisecond precision")
    void convertsTime() {
        assertThat(CudEventFactory.normalizeTemporal(45045123, Time.schema())).isEqualTo("12:30:45.123Z");
    }

    @Test
    @DisplayName("Converts io.debezium.time.MicroTime (micros since midnight) to a UTC ISO time with microsecond precision")
    void convertsMicroTime() {
        assertThat(CudEventFactory.normalizeTemporal(45045123456L, MicroTime.schema())).isEqualTo("12:30:45.123456Z");
    }

    @Test
    @DisplayName("Converts io.debezium.time.NanoTime (nanos since midnight) to a UTC ISO time with nanosecond precision")
    void convertsNanoTime() {
        assertThat(CudEventFactory.normalizeTemporal(45045123456789L, NanoTime.schema())).isEqualTo("12:30:45.123456789Z");
    }

    @Test
    @DisplayName("Converts a connect-mode Date (java.util.Date) to an ISO date string")
    void convertsConnectDate() {
        final var value = java.util.Date.from(Instant.parse("2024-01-01T00:00:00Z"));
        assertThat(CudEventFactory.normalizeTemporal(value, org.apache.kafka.connect.data.Date.SCHEMA))
                .isEqualTo("2024-01-01");
    }

    @Test
    @DisplayName("Converts a connect-mode Time (java.util.Date) to a UTC ISO time string")
    void convertsConnectTime() {
        final var value = java.util.Date.from(Instant.parse("1970-01-01T12:30:45.123Z"));
        assertThat(CudEventFactory.normalizeTemporal(value, org.apache.kafka.connect.data.Time.SCHEMA))
                .isEqualTo("12:30:45.123Z");
    }

    @Test
    @DisplayName("Converts a connect-mode Timestamp (java.util.Date) to a UTC ISO datetime string")
    void convertsConnectTimestamp() {
        final var value = java.util.Date.from(Instant.parse("2024-01-01T12:30:45.123Z"));
        assertThat(CudEventFactory.normalizeTemporal(value, org.apache.kafka.connect.data.Timestamp.SCHEMA))
                .isEqualTo("2024-01-01T12:30:45.123Z");
    }

    @Test
    @DisplayName("Leaves ZonedTime untouched since it already arrives as an ISO string")
    void passesZonedTimeThrough() {
        assertThat(CudEventFactory.normalizeTemporal("12:30:45Z", ZonedTime.schema())).isEqualTo("12:30:45Z");
    }

    @Test
    @DisplayName("Normalizes timestamps to UTC with a trailing Z, matching Debezium's isostring convention")
    void timestampIsUtcWithZ() {
        assertThat((String) CudEventFactory.normalizeTemporal(0L, Timestamp.schema())).isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    @DisplayName("Date is rendered without an offset so it is valid Neo4j date() input")
    void dateHasNoOffset() {
        assertThat((String) CudEventFactory.normalizeTemporal(19723, Date.schema())).doesNotContain("Z", "+");
    }

    @Test
    @DisplayName("Leaves ZonedTimestamp untouched since it already arrives as an ISO string")
    void passesZonedTimestampThrough() {
        assertThat(CudEventFactory.normalizeTemporal("2024-01-01T12:30:45Z", ZonedTimestamp.schema()))
                .isEqualTo("2024-01-01T12:30:45Z");
    }

    @Test
    @DisplayName("Passes a plain INT64 (no logical name) through unchanged")
    void passesPlainLongThrough() {
        assertThat(CudEventFactory.normalizeTemporal(1704112245123L, Schema.INT64_SCHEMA)).isEqualTo(1704112245123L);
    }

    @Test
    @DisplayName("Returns null unchanged")
    void passesNullThrough() {
        assertThat(CudEventFactory.normalizeTemporal(null, Date.schema())).isNull();
    }

    @Test
    @DisplayName("Returns the value unchanged when the schema is null")
    void passesThroughWhenSchemaNull() {
        assertThat(CudEventFactory.normalizeTemporal(42, null)).isEqualTo(42);
    }
}
