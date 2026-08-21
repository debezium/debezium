/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.BinaryHandlingMode;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the {@code binary.handling.mode} configuration and its per-field selectors.
 *
 * @author Minjae Lee
 */
class JdbcSinkConnectorConfigBinaryHandlingModeTest {

    private static final byte[] NON_UTF8_BYTES = { (byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0 };

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("binary.handling.mode defaults to bytes")
    void shouldDefaultToBytes() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of());
        assertThat(config.getBinaryHandlingMode()).isEqualTo(BinaryHandlingMode.BYTES);
        assertThat(config.getBinaryHandlingMode("topic", "data")).isEqualTo(BinaryHandlingMode.BYTES);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("binary.handling.mode parses all supported values")
    void shouldParseAllModes() {
        assertThat(mode("bytes")).isEqualTo(BinaryHandlingMode.BYTES);
        assertThat(mode("base64")).isEqualTo(BinaryHandlingMode.BASE64);
        assertThat(mode("base64-url-safe")).isEqualTo(BinaryHandlingMode.BASE64_URL_SAFE);
        assertThat(mode("hex")).isEqualTo(BinaryHandlingMode.HEX);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("selectors override the global mode by field name")
    void shouldResolveSelectorByFieldName() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "base64",
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_HEX, "data_hex",
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_BYTES, "data_raw.*"));

        assertThat(config.getBinaryHandlingMode("topic", "data_hex")).isEqualTo(BinaryHandlingMode.HEX);
        assertThat(config.getBinaryHandlingMode("topic", "data_raw_1")).isEqualTo(BinaryHandlingMode.BYTES);
        assertThat(config.getBinaryHandlingMode("topic", "other")).isEqualTo(BinaryHandlingMode.BASE64);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("selectors match topic-qualified <topic>:<field> names")
    void shouldResolveSelectorByTopicQualifiedName() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_BASE64_URL_SAFE, "server1\\.inventory\\.orders:payload"));

        assertThat(config.getBinaryHandlingMode("server1.inventory.orders", "payload")).isEqualTo(BinaryHandlingMode.BASE64_URL_SAFE);
        assertThat(config.getBinaryHandlingMode("server1.inventory.other", "payload")).isEqualTo(BinaryHandlingMode.BYTES);
        assertThat(config.getBinaryHandlingMode("server1.inventory.orders", "other")).isEqualTo(BinaryHandlingMode.BYTES);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("selector evaluation order is base64, base64-url-safe, hex, bytes")
    void shouldResolveOverlappingSelectorsInFixedOrder() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_HEX, "data.*",
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_BASE64, "data_b64"));

        assertThat(config.getBinaryHandlingMode("topic", "data_b64")).isEqualTo(BinaryHandlingMode.BASE64);
        assertThat(config.getBinaryHandlingMode("topic", "data_other")).isEqualTo(BinaryHandlingMode.HEX);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("selector matching is case-insensitive")
    void shouldMatchSelectorsCaseInsensitively() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_HEX, "DATA"));

        assertThat(config.getBinaryHandlingMode("topic", "data")).isEqualTo(BinaryHandlingMode.HEX);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("textual modes encode bytes deterministically")
    void shouldEncodeBytes() {
        assertThat(BinaryHandlingMode.BASE64.encode(NON_UTF8_BYTES)).isEqualTo("/9j/4A==");
        assertThat(BinaryHandlingMode.BASE64_URL_SAFE.encode(NON_UTF8_BYTES)).isEqualTo("_9j_4A==");
        assertThat(BinaryHandlingMode.HEX.encode(NON_UTF8_BYTES)).isEqualTo("ffd8ffe0");
        assertThatThrownBy(() -> BinaryHandlingMode.BYTES.encode(NON_UTF8_BYTES))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("invalid selector regex fails validation")
    void shouldFailValidationOnInvalidSelectorRegex() {
        final Map<String, String> props = new HashMap<>();
        props.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc:mysql://localhost:3306/db");
        props.put(JdbcSinkConnectorConfig.CONNECTION_USER, "user");
        props.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, "pass");
        props.put(JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_HEX, "data([");

        assertThatThrownBy(() -> new JdbcSinkConnectorConfig(props).validate())
                .isInstanceOf(ConnectException.class);
    }

    @Test
    @FixFor("debezium/dbz#2468")
    @DisplayName("binary handling is reported enabled only when a mode or selector is configured")
    void shouldReportBinaryHandlingEnabledOnlyWhenConfigured() {
        assertThat(new JdbcSinkConnectorConfig(Map.of()).isBinaryHandlingEnabled()).isFalse();
        assertThat(new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "bytes")).isBinaryHandlingEnabled()).isFalse();
        assertThat(new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, "hex")).isBinaryHandlingEnabled()).isTrue();
        assertThat(new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_BASE64, "data")).isBinaryHandlingEnabled()).isTrue();
    }

    private static BinaryHandlingMode mode(String value) {
        return new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, value)).getBinaryHandlingMode();
    }
}
