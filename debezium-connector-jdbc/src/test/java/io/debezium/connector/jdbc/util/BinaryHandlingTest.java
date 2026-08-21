/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.BinaryHandlingMode;
import io.debezium.doc.FixFor;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;

/**
 * Unit tests for the {@link BinaryHandling} resolution logic.
 *
 * @author Minjae Lee
 */
class BinaryHandlingTest {

    private static final String TOPIC = "server1.inventory.orders";

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("resolves a textual mode for plain BYTES fields targeting character columns")
    void shouldResolveTextualModeForPlainBytesFieldWithCharacterTarget() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "base64"));

        assertThat(BinaryHandling.resolve(config, TOPIC, bytesField(), column(Types.VARCHAR))).isEqualTo(BinaryHandlingMode.BASE64);
        assertThat(BinaryHandling.resolve(config, TOPIC, bytesField(), column(Types.CLOB))).isEqualTo(BinaryHandlingMode.BASE64);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("resolves bytes for binary targets, missing columns, and the default configuration")
    void shouldResolveBytesOutsideCharacterTargetsWithTextualMode() {
        final JdbcSinkConnectorConfig textualConfig = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "base64"));
        final JdbcSinkConnectorConfig defaultConfig = new JdbcSinkConnectorConfig(Map.of());

        assertThat(BinaryHandling.resolve(textualConfig, TOPIC, bytesField(), column(Types.VARBINARY))).isEqualTo(BinaryHandlingMode.BYTES);
        assertThat(BinaryHandling.resolve(textualConfig, TOPIC, bytesField(), null)).isEqualTo(BinaryHandlingMode.BYTES);
        assertThat(BinaryHandling.resolve(defaultConfig, TOPIC, bytesField(), column(Types.VARCHAR))).isEqualTo(BinaryHandlingMode.BYTES);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("resolves bytes for logical types that use the BYTES schema type")
    void shouldResolveBytesForLogicalBytesSchemas() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "base64"));
        final FieldDescriptor decimalField = new FieldDescriptor(Decimal.schema(2), "data", false);

        assertThat(BinaryHandling.resolve(config, TOPIC, decimalField, column(Types.VARCHAR))).isEqualTo(BinaryHandlingMode.BYTES);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("selectors decide the mode per field")
    void shouldFollowSelectorResolution() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "base64",
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_BYTES, "data"));

        assertThat(BinaryHandling.resolve(config, TOPIC, bytesField(), column(Types.VARCHAR))).isEqualTo(BinaryHandlingMode.BYTES);
        assertThat(BinaryHandling.resolve(config, TOPIC, new FieldDescriptor(Schema.OPTIONAL_BYTES_SCHEMA, "other", false),
                column(Types.VARCHAR))).isEqualTo(BinaryHandlingMode.BASE64);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("character type detection covers all character JDBC types")
    void shouldDetectCharacterTypes() {
        assertThat(BinaryHandling.isCharacterType(Types.CHAR)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.VARCHAR)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.LONGVARCHAR)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.NCHAR)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.NVARCHAR)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.LONGNVARCHAR)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.CLOB)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.NCLOB)).isTrue();
        assertThat(BinaryHandling.isCharacterType(Types.BINARY)).isFalse();
        assertThat(BinaryHandling.isCharacterType(Types.VARBINARY)).isFalse();
        assertThat(BinaryHandling.isCharacterType(Types.BLOB)).isFalse();
        assertThat(BinaryHandling.isCharacterType(Types.INTEGER)).isFalse();
    }

    private static FieldDescriptor bytesField() {
        return new FieldDescriptor(Schema.OPTIONAL_BYTES_SCHEMA, "data", false);
    }

    private static ColumnDescriptor column(int jdbcType) {
        return ColumnDescriptor.builder()
                .columnName("data")
                .jdbcType(jdbcType)
                .typeName("any")
                .build();
    }
}
