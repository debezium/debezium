/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.util.Strings;

/**
 * Utility helper class for HSTORE column data types.
 *
 * @author Chris Cranford
 */
public class HstoreConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Converts a JSON-based string to a HStORE string.
     *
     * @param value JSON-based string
     * @return the HSTORE column type value represented as a string
     */
    public static String jsonToString(String value) {
        if (Strings.isNullOrBlank(value)) {
            return null;
        }
        try {
            final Map<String, String> hstoreMap = new HashMap<>();
            final JsonNode json = MAPPER.readTree(value);
            for (Iterator<String> iterator = json.fieldNames(); iterator.hasNext();) {
                final String fieldName = iterator.next();
                final String fieldValue = json.get(fieldName).textValue();
                hstoreMap.put(fieldName, fieldValue);
            }
            return mapToString(hstoreMap);
        }
        catch (JsonProcessingException e) {
            throw new ConnectException("Failed to deserialize JSON to HSTORE", e);
        }
    }

    /**
     * Converts a Java-based map to a HSTORE string representation.
     *
     * @param hstoreMap map of key/value tuples, should not be {@code null}.
     * @return the HSTORE column type value represented as a string
     */
    public static String mapToString(Map<String, String> hstoreMap) {
        if (hstoreMap == null) {
            return null;
        }
        return hstoreMap.entrySet().stream()
                .map(entry -> formatHstoreEntry(entry))
                .collect(Collectors.joining(", "));
    }

    private static String formatHstoreEntry(Map.Entry<String, String> entry) {
        return String.format("\"%s\" => \"%s\"", entry.getKey(), entry.getValue());
    }

}
