/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

import org.bson.BsonTimestamp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Parses a change stream start time without losing timestamp precision or overflowing BSON timestamp components.
 */
final class BsonTimestampParser {

    private static final long MAX_UNSIGNED_INT = 0xffff_ffffL;
    private static final Pattern UNIX_SECONDS = Pattern.compile("[+-]?[0-9]+");
    private static final String FORMAT_DESCRIPTION = "Expected integer Unix seconds, an ISO-8601 timestamp with a UTC offset "
            + "and whole-second precision, or an Extended JSON timestamp such as {\"$timestamp\":{\"t\":30,\"i\":0}}";
    private static final ObjectReader JSON_READER = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
            .build().reader();

    private BsonTimestampParser() {
    }

    static BsonTimestamp parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(FORMAT_DESCRIPTION);
        }
        final var timestamp = value.trim();
        try {
            if (UNIX_SECONDS.matcher(timestamp).matches()) {
                return new BsonTimestamp(unsignedInt(Long.parseLong(timestamp), "Unix seconds"), 0);
            }
            if (timestamp.startsWith("{")) {
                return parseExtendedJson(timestamp);
            }
            final var instant = OffsetDateTime.parse(timestamp).toInstant();
            if (instant.getNano() != 0) {
                throw new IllegalArgumentException("Sub-second precision is not supported; specify a whole-second timestamp");
            }
            return new BsonTimestamp(unsignedInt(instant.getEpochSecond(), "Unix seconds"), 0);
        }
        catch (NumberFormatException | DateTimeParseException | JsonProcessingException e) {
            throw new IllegalArgumentException(FORMAT_DESCRIPTION, e);
        }
    }

    private static BsonTimestamp parseExtendedJson(String value) throws JsonProcessingException {
        final JsonNode root = JSON_READER.readTree(value);
        final var timestamp = root.get("$timestamp");
        if (root.size() != 1 || timestamp == null || !timestamp.isObject() || timestamp.size() != 2
                || !timestamp.has("t") || !timestamp.has("i")) {
            throw new IllegalArgumentException(FORMAT_DESCRIPTION);
        }
        return new BsonTimestamp(unsignedInt(timestamp.get("t"), "t"), unsignedInt(timestamp.get("i"), "i"));
    }

    private static int unsignedInt(JsonNode value, String component) {
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new IllegalArgumentException("Timestamp component '" + component + "' must be an integer between 0 and " + MAX_UNSIGNED_INT);
        }
        return unsignedInt(value.longValue(), component);
    }

    private static int unsignedInt(long value, String component) {
        if (value < 0 || value > MAX_UNSIGNED_INT) {
            throw new IllegalArgumentException("Timestamp component '" + component + "' must be between 0 and " + MAX_UNSIGNED_INT);
        }
        // BSON uses unsigned 32-bit components, represented as signed ints by the Java driver.
        return (int) value;
    }
}
