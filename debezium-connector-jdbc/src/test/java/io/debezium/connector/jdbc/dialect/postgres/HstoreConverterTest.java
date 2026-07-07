/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for the PostgreSQL {@link HstoreConverter} helper.
 */
@Tag("UnitTests")
class HstoreConverterTest {

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should quote plain keys and values")
    void testMapToStringQuotesEntries() {
        final Map<String, String> map = new LinkedHashMap<>();
        map.put("key", "value");
        assertThat(HstoreConverter.mapToString(map)).isEqualTo("\"key\" => \"value\"");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should escape double quotes in keys and values")
    void testMapToStringEscapesDoubleQuotes() {
        final Map<String, String> map = new LinkedHashMap<>();
        map.put("na\"me", "a \" b");
        assertThat(HstoreConverter.mapToString(map)).isEqualTo("\"na\\\"me\" => \"a \\\" b\"");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should escape backslashes in keys and values")
    void testMapToStringEscapesBackslashes() {
        final Map<String, String> map = new LinkedHashMap<>();
        map.put("pa\\th", "c:\\tmp");
        assertThat(HstoreConverter.mapToString(map)).isEqualTo("\"pa\\\\th\" => \"c:\\\\tmp\"");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should render null values as the unquoted NULL keyword")
    void testMapToStringRendersNullValueAsKeyword() {
        final Map<String, String> map = new LinkedHashMap<>();
        map.put("nullkey", null);
        assertThat(HstoreConverter.mapToString(map)).isEqualTo("\"nullkey\" => NULL");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should render the literal string NULL as a quoted value")
    void testMapToStringRendersLiteralNullStringQuoted() {
        final Map<String, String> map = new LinkedHashMap<>();
        map.put("key", "NULL");
        assertThat(HstoreConverter.mapToString(map)).isEqualTo("\"key\" => \"NULL\"");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should return null for a null map")
    void testMapToStringReturnsNullForNullMap() {
        assertThat(HstoreConverter.mapToString(null)).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should escape quotes and backslashes when converting from JSON")
    void testJsonToStringEscapesSpecialCharacters() {
        assertThat(HstoreConverter.jsonToString("{\"quote\":\"a \\\" b\"}"))
                .isEqualTo("\"quote\" => \"a \\\" b\"");
        assertThat(HstoreConverter.jsonToString("{\"path\":\"c:\\\\tmp\"}"))
                .isEqualTo("\"path\" => \"c:\\\\tmp\"");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should render JSON null values as the unquoted NULL keyword")
    void testJsonToStringRendersNullValueAsKeyword() {
        assertThat(HstoreConverter.jsonToString("{\"nullkey\":null}"))
                .isEqualTo("\"nullkey\" => NULL");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should return null for a null or blank JSON string")
    void testJsonToStringReturnsNullForBlank() {
        assertThat(HstoreConverter.jsonToString(null)).isNull();
        assertThat(HstoreConverter.jsonToString("")).isNull();
    }
}
