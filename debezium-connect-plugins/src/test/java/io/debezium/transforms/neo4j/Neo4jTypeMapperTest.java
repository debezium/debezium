/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link Neo4jTypeMapper}: how a Neo4j CDC property value (a {@code Neo4jPropertyType} union struct in
 * EXTENDED payload mode) maps to a flat relational column (schema + value) for the Debezium JDBC sink.
 */
class Neo4jTypeMapperTest {

    @Test
    @DisplayName("an integer maps to an optional INT64 column")
    void integer() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.i64(42L));
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(converted.schema().isOptional()).isTrue();
        assertThat(converted.value()).isEqualTo(42L);
    }

    @Test
    @DisplayName("a float maps to an optional FLOAT64 column")
    void floatingPoint() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.f64(9.5d));
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(converted.value()).isEqualTo(9.5d);
    }

    @Test
    @DisplayName("a boolean maps to an optional BOOLEAN column")
    void booleanValue() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.bool(true));
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(converted.value()).isEqualTo(true);
    }

    @Test
    @DisplayName("a string maps to an optional STRING column")
    void string() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.string("Alice"));
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(converted.value()).isEqualTo("Alice");
    }

    @Test
    @DisplayName("a Neo4j date maps to io.debezium.time.Date (epoch days)")
    void date() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.date("1990-01-15"));
        assertThat(converted.schema().name()).isEqualTo("io.debezium.time.Date");
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(converted.value()).isEqualTo((int) LocalDate.of(1990, 1, 15).toEpochDay());
    }

    @Test
    @DisplayName("a Neo4j local datetime maps to io.debezium.time.Timestamp (epoch millis)")
    void localDateTime() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.localDateTime("2021-06-15T10:15:30"));
        assertThat(converted.schema().name()).isEqualTo("io.debezium.time.Timestamp");
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(converted.value())
                .isEqualTo(LocalDateTime.of(2021, 6, 15, 10, 15, 30).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    @DisplayName("a Neo4j local time maps to io.debezium.time.MicroTime (micros of day)")
    void localTime() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.localTime("08:30:00"));
        assertThat(converted.schema().name()).isEqualTo("io.debezium.time.MicroTime");
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(converted.value()).isEqualTo(LocalTime.of(8, 30, 0).toNanoOfDay() / 1_000L);
    }

    @Test
    @DisplayName("a Neo4j zoned datetime maps to io.debezium.time.ZonedTimestamp (ISO string)")
    void zonedDateTime() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.zonedDateTime("2021-06-15T10:15:30+01:00"));
        assertThat(converted.schema().name()).isEqualTo("io.debezium.time.ZonedTimestamp");
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(converted.value()).isEqualTo("2021-06-15T10:15:30+01:00");
    }

    @Test
    @DisplayName("a Neo4j offset time maps to io.debezium.time.ZonedTime (ISO string)")
    void offsetTime() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.offsetTime("12:30:00+01:00"));
        assertThat(converted.schema().name()).isEqualTo("io.debezium.time.ZonedTime");
        assertThat(converted.value()).isEqualTo("12:30:00+01:00");
    }

    @Test
    @FixFor("debezium/dbz#DDD-74")
    @DisplayName("a Neo4j duration maps to an ISO-8601 duration STRING")
    void duration() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.duration(14, 3, 14706, 0));
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(converted.value()).isEqualTo("P14M3DT4H5M6S");
    }

    @Test
    @FixFor("debezium/dbz#DDD-74")
    @DisplayName("a Neo4j point maps to a JSON STRING with srid and coordinates")
    void point() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.point(4326, 56.78, 12.34));
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) converted.value()).contains("\"srid\":4326").contains("\"x\":56.78").contains("\"y\":12.34");
    }

    @Test
    @DisplayName("a homogeneous list of primitives stays an array column")
    void primitiveArray() {
        final var converted = Neo4jTypeMapper.convert(Neo4jPropertyTypesFixture.stringList(List.of("a", "b")));
        assertThat(converted.schema().type()).isEqualTo(Schema.Type.ARRAY);
        assertThat(converted.schema().valueSchema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(converted.value()).isEqualTo(List.of("a", "b"));
    }
}
