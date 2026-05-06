/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class SchemaUtilsTest {

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnColumnTypeWhenPresent() {
        Schema schema = SchemaBuilder.string()
                .parameters(Map.of("__debezium.source.column.type", "VARCHAR"))
                .build();

        assertThat(SchemaUtils.getSourceColumnType(schema)).isEqualTo(Optional.of("VARCHAR"));
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnEmptyWhenColumnTypeAbsent() {
        assertThat(SchemaUtils.getSourceColumnType(Schema.STRING_SCHEMA)).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnColumnSize() {
        Schema schema = SchemaBuilder.string()
                .parameters(Map.of("__debezium.source.column.length", "255"))
                .build();

        assertThat(SchemaUtils.getSourceColumnSize(schema)).isEqualTo(Optional.of("255"));
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnColumnPrecision() {
        Schema schema = SchemaBuilder.string()
                .parameters(Map.of("__debezium.source.column.scale", "2"))
                .build();

        assertThat(SchemaUtils.getSourceColumnPrecision(schema)).isEqualTo(Optional.of("2"));
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnColumnName() {
        Schema schema = SchemaBuilder.string()
                .parameters(Map.of("__debezium.source.column.name", "original_name"))
                .build();

        assertThat(SchemaUtils.getSourceColumnName(schema)).isEqualTo(Optional.of("original_name"));
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnEmptyForSchemaWithNoParameters() {
        assertThat(SchemaUtils.getSchemaParameter(Schema.INT32_SCHEMA, "any.param")).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnEmptyForMissingParameter() {
        Schema schema = SchemaBuilder.string()
                .parameters(Map.of("other.param", "value"))
                .build();

        assertThat(SchemaUtils.getSchemaParameter(schema, "__debezium.source.column.type")).isEmpty();
    }
}
