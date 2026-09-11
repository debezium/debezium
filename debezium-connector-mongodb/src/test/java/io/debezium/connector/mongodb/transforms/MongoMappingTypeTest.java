/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.VariableScaleDecimal;

class MongoMappingTypeTest {
    private static final String DATE = "{\"$date\":\"2026-09-11T12:34:56.123Z\"}";
    private static final long EPOCH_MILLIS = Instant.parse("2026-09-11T12:34:56.123Z").toEpochMilli();

    static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of("int8", "42", (byte) 42),
                Arguments.of("int16", "300", (short) 300),
                Arguments.of("int32", "{\"$numberLong\":\"42\"}", 42),
                Arguments.of("int64", "42", 42L),
                Arguments.of("float32", "1.25", 1.25f),
                Arguments.of("float64", "42", 42d),
                Arguments.of("boolean", "true", true),
                Arguments.of("boolean", "\"FALSE\"", false),
                Arguments.of("string", "{\"$oid\":\"507f1f77bcf86cd799439011\"}", "507f1f77bcf86cd799439011"),
                Arguments.of("string", "{\"$numberDecimal\":\"19.99\"}", "19.99"),
                Arguments.of("bytes", "{\"$binary\":{\"base64\":\"AQID\",\"subType\":\"00\"}}", new byte[]{ 1, 2, 3 }),
                Arguments.of("io.debezium.time.Date", DATE, (int) LocalDate.of(2026, 9, 11).toEpochDay()),
                Arguments.of("io.debezium.time.Timestamp", DATE, EPOCH_MILLIS),
                Arguments.of("io.debezium.time.MicroTimestamp", DATE, EPOCH_MILLIS * 1_000),
                Arguments.of("io.debezium.time.NanoTimestamp", DATE, EPOCH_MILLIS * 1_000_000),
                Arguments.of("io.debezium.time.Time", DATE, 45_296_123),
                Arguments.of("io.debezium.time.MicroTime", DATE, 45_296_123_000L),
                Arguments.of("io.debezium.time.NanoTime", DATE, 45_296_123_000_000L),
                Arguments.of("io.debezium.time.ZonedTimestamp", DATE, "2026-09-11T12:34:56.123Z"),
                Arguments.of("io.debezium.time.ZonedTime", DATE, "12:34:56.123Z"),
                Arguments.of("io.debezium.time.Year", DATE, 2026),
                Arguments.of("io.debezium.time.MicroDuration", "\"PT1.000001S\"", 1_000_001L),
                Arguments.of("io.debezium.time.NanoDuration", "\"PT1.000000001S\"", 1_000_000_001L),
                Arguments.of("org.apache.kafka.connect.data.Date", DATE, Date.from(Instant.parse("2026-09-11T00:00:00Z"))),
                Arguments.of("org.apache.kafka.connect.data.Time", DATE, new Date(45_296_123)),
                Arguments.of("org.apache.kafka.connect.data.Timestamp", DATE, new Date(EPOCH_MILLIS)),
                Arguments.of("io.debezium.data.Uuid", "\"81be2114-44cb-4f92-8b54-72212b691014\"", "81be2114-44cb-4f92-8b54-72212b691014"),
                Arguments.of("io.debezium.data.Uuid", "{\"$binary\":{\"base64\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"subType\":\"04\"}}",
                        "00000000-0000-0000-0000-000000000000"),
                Arguments.of("integer", "42", 42), Arguments.of("long", "\"42\"", 42L),
                Arguments.of("float", "1.25", 1.25f), Arguments.of("double", "1.25", 1.25d),
                Arguments.of("INT64", "42", 42L),
                Arguments.of("io.debezium.time.Date", "\"1969-12-31\"", -1),
                Arguments.of("io.debezium.time.Timestamp", "\"1969-12-31T23:59:59.999Z\"", -1L),
                Arguments.of("io.debezium.time.MicroTimestamp", "\"1970-01-01T01:00:00.000001+01:00\"", 1L),
                Arguments.of("io.debezium.time.NanoTimestamp", "\"1970-01-01T00:00:00.000000001Z\"", 1L),
                Arguments.of("io.debezium.time.Time", "\"00:00:00.001\"", 1),
                Arguments.of("io.debezium.time.MicroTime", "\"00:00:00.000001\"", 1L),
                Arguments.of("io.debezium.time.NanoTime", "\"00:00:00.000000001\"", 1L),
                Arguments.of("io.debezium.time.ZonedTimestamp", "\"2026-09-11T12:34:56+09:00\"", "2026-09-11T12:34:56+09:00"),
                Arguments.of("io.debezium.time.ZonedTime", "\"12:34:56+09:00\"", "12:34:56+09:00"),
                Arguments.of("io.debezium.time.Year", "\"2026\"", 2026),
                Arguments.of("io.debezium.time.MicroDuration", "1250", 1250L),
                Arguments.of("io.debezium.time.NanoDuration", "1000", 1000L),
                Arguments.of("io.debezium.time.Date", "-1", -1),
                Arguments.of("io.debezium.time.Timestamp", "123", 123L),
                Arguments.of("io.debezium.time.Time", "123", 123),
                Arguments.of("io.debezium.time.Timestamp", "{\"$timestamp\":{\"t\":4294967295,\"i\":7}}", 4_294_967_295_000L));
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldConvertValuesToTheDeclaredConnectRepresentation(String type, String json, Object expected) {
        final var schema = MongoMappingType.schema(type, null, null, null);
        final var converted = MongoMappingType.convert(BsonDocument.parse("{\"v\":" + json + "}").get("v"), schema);
        final var record = new Struct(SchemaBuilder.struct().field("v", schema).build()).put("v", converted);
        record.validate();
        if (expected instanceof byte[] bytes) {
            assertThat((byte[]) converted).containsExactly(bytes);
        }
        else {
            assertThat(converted).isEqualTo(expected);
        }
    }

    @Test
    void shouldPreserveDecimalPrecisionAndScale() {
        final var value = BsonDocument.parse("{\"price\":{\"$numberDecimal\":\"12345678901234567890123456789012.34\"}}").get("price");
        final var fixed = MongoMappingType.schema("org.apache.kafka.connect.data.Decimal", 2, 34, null);
        assertThat(MongoMappingType.convert(value, fixed)).isEqualTo(new BigDecimal("12345678901234567890123456789012.34"));
        final var variable = MongoMappingType.schema("io.debezium.data.VariableScaleDecimal", null, null, null);
        final var converted = (Struct) MongoMappingType.convert(value, variable);
        converted.validate();
        assertThat(VariableScaleDecimal.toLogical(converted).getDecimalValue()).contains(new BigDecimal("12345678901234567890123456789012.34"));
        assertThat(MongoMappingType.convert(new BsonString("12.3"), fixed)).isEqualTo(new BigDecimal("12.30"));
    }

    @Test
    void shouldSerializeSelectedBsonValuesAsExtendedJson() {
        final var schema = MongoMappingType.schema("io.debezium.data.Json", null, null, null);
        for (String json : new String[]{ "{\"a\":{\"$numberDecimal\":\"19.99\"}}", "[1,\"x\",true]", "{\"$timestamp\":{\"t\":10,\"i\":7}}", "\"hello\"" }) {
            final var original = BsonDocument.parse("{\"v\":" + json + "}").get("v");
            final var serialized = (String) MongoMappingType.convert(original, schema);
            assertThat(BsonDocument.parse("{\"v\":" + serialized + "}").get("v")).isEqualTo(original);
        }
    }

    @Test
    void shouldValidateBitsLength() {
        final var schema = MongoMappingType.schema("io.debezium.data.Bits", null, null, 9);
        final var value = BsonDocument.parse("{\"v\":{\"$binary\":{\"base64\":\"/wE=\",\"subType\":\"00\"}}}").get("v");
        assertThat((byte[]) MongoMappingType.convert(value, schema)).containsExactly((byte) 255, (byte) 1);
        assertThatThrownBy(() -> MongoMappingType.convert(value, MongoMappingType.schema("io.debezium.data.Bits", null, null, 8)))
                .isInstanceOf(DataException.class);
    }

    static Stream<Arguments> invalidValues() {
        return Stream.of(
                Arguments.of("int8", "128"), Arguments.of("int16", "32768"),
                Arguments.of("int32", "{\"$numberLong\":\"2147483648\"}"),
                Arguments.of("int64", "\"9223372036854775808\""), Arguments.of("int32", "1.5"),
                Arguments.of("int32", "\"abc\""), Arguments.of("int32", "true"),
                Arguments.of("float32", "\"1e100\""), Arguments.of("float32", "\"1e-100\""),
                Arguments.of("float64", "\"1e1000\""), Arguments.of("float64", "\"1e-1000\""),
                Arguments.of("float64", "{\"$numberDouble\":\"NaN\"}"),
                Arguments.of("boolean", "\"yes\""), Arguments.of("boolean", "1"),
                Arguments.of("string", "{\"a\":1}"), Arguments.of("bytes", "\"abc\""),
                Arguments.of("io.debezium.data.Uuid", "\"1-1-1-1-1\""),
                Arguments.of("io.debezium.data.Uuid", "{\"$binary\":{\"base64\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"subType\":\"03\"}}"),
                Arguments.of("io.debezium.time.Timestamp", "\"2026-09-11T00:00:00\""),
                Arguments.of("io.debezium.time.Timestamp", "\"2026-09-11T00:00:00.000001Z\""),
                Arguments.of("io.debezium.time.MicroTimestamp", "\"2026-09-11T00:00:00.000000001Z\""),
                Arguments.of("io.debezium.time.NanoTimestamp", "\"2500-01-01T00:00:00Z\""),
                Arguments.of("io.debezium.time.Time", "86400000"), Arguments.of("io.debezium.time.Time", "-1"),
                Arguments.of("io.debezium.time.Time", "\"00:00:00.000001\""),
                Arguments.of("io.debezium.time.Year", "1000000000"), Arguments.of("io.debezium.time.Date", "\"invalid\""),
                Arguments.of("io.debezium.time.Timestamp", "true"),
                Arguments.of("io.debezium.time.MicroDuration", "1.25"),
                Arguments.of("io.debezium.time.MicroDuration", "\"PT0.000000001S\""));
    }

    @ParameterizedTest
    @MethodSource("invalidValues")
    void shouldReportConversionErrorsWithTheOutputField(String type, String json) throws JsonProcessingException {
        final var mapping = new MongoDocumentMapping("shop.orders", new ObjectMapper().writeValueAsString(Map.of("selected", Map.of("path", "/v", "type", type))));
        final var document = BsonDocument.parse("{\"v\":" + json + "}");
        assertThatThrownBy(() -> mapping.convert(document, document.toJson()))
                .isInstanceOf(DataException.class).hasMessageContaining("selected");
    }

    @Test
    void shouldRejectDecimalRoundingOverflowAndNonFiniteNumbers() {
        final var schema = MongoMappingType.schema("org.apache.kafka.connect.data.Decimal", 2, 5, null);
        for (String value : new String[]{ "19.999", "1000.00", "NaN", "Infinity" }) {
            assertThatThrownBy(() -> MongoMappingType.convert(new BsonString(value), schema)).isInstanceOf(RuntimeException.class);
        }
    }
}
