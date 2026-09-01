/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;

final class ClaimCheckRecordSerializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ClaimCheckRecordSerializer() {
    }

    static SerializedRecord serialize(SourceRecord record) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("version", 1);
        payload.put("topic", record.topic());
        payload.put("sourcePartition", canonicalMap(record.sourcePartition()));
        payload.put("sourceOffset", canonicalMap(record.sourceOffset()));
        payload.put("headers", serializeHeaders(record));
        payload.put("key", connectValue(record.keySchema(), record.key()));
        payload.put("value", connectValue(record.valueSchema(), record.value()));

        try {
            byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(payload);
            String payloadHash = sha256Hex(bytes);
            Map<String, Object> sourcePosition = new LinkedHashMap<>();
            sourcePosition.put("sourcePartition", canonicalMap(record.sourcePartition()));
            sourcePosition.put("sourceOffset", canonicalMap(record.sourceOffset()));
            String offsetHash = sha256Hex(OBJECT_MAPPER.writeValueAsBytes(sourcePosition));
            String key = sanitize(record.topic()) + "/offset-" + offsetHash.substring(0, 16)
                    + "-sha256-" + payloadHash + ".json";
            return new SerializedRecord(key, bytes, payloadHash);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Failed to serialize oversized record to JSON", e);
        }
    }

    private static List<Map<String, Object>> serializeHeaders(SourceRecord record) {
        List<Map<String, Object>> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            Map<String, Object> serializedHeader = new LinkedHashMap<>();
            serializedHeader.put("key", header.key());
            serializedHeader.put("value", connectValue(header.schema(), header.value()));
            headers.add(serializedHeader);
        }
        return headers;
    }

    @SuppressWarnings("unchecked")
    private static Object connectValue(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        if (schema == null) {
            return canonicalValue(value);
        }

        return switch (schema.type()) {
            case STRUCT -> {
                Struct struct = (Struct) value;
                Map<String, Object> object = new LinkedHashMap<>();
                for (org.apache.kafka.connect.data.Field field : schema.fields()) {
                    object.put(field.name(), connectValue(field.schema(), struct.getWithoutDefault(field.name())));
                }
                yield object;
            }
            case ARRAY -> {
                List<Object> values = new ArrayList<>();
                for (Object element : (List<Object>) value) {
                    values.add(connectValue(schema.valueSchema(), element));
                }
                yield values;
            }
            case MAP -> {
                Map<Object, Object> map = (Map<Object, Object>) value;
                Map<String, Object> mapped = new TreeMap<>();
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    mapped.put(String.valueOf(entry.getKey()), connectValue(schema.valueSchema(), entry.getValue()));
                }
                yield mapped;
            }
            case BYTES -> Base64.getEncoder().encodeToString(bytes(value));
            case STRING -> value.toString();
            default -> value instanceof BigDecimal ? value.toString() : value;
        };
    }

    private static Map<String, Object> canonicalMap(Map<String, ?> value) {
        if (value == null || value.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> canonical = new TreeMap<>();
        value.forEach((key, item) -> canonical.put(key, canonicalValue(item)));
        return canonical;
    }

    private static Object canonicalValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> canonical = new TreeMap<>();
            map.entrySet().stream()
                    .sorted(Comparator.comparing(entry -> String.valueOf(entry.getKey())))
                    .forEach(entry -> canonical.put(String.valueOf(entry.getKey()), canonicalValue(entry.getValue())));
            return canonical;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(ClaimCheckRecordSerializer::canonicalValue).toList();
        }
        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof ByteBuffer buffer) {
            return Base64.getEncoder().encodeToString(bytes(buffer));
        }
        return value;
    }

    private static byte[] bytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            byte[] data = new byte[copy.remaining()];
            copy.get(data);
            return data;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.unscaledValue().toByteArray();
        }
        throw new DebeziumException("Unsupported BYTES value type: " + value.getClass().getName());
    }

    private static String sanitize(String value) {
        String candidate = Strings.isNullOrBlank(value) ? "unknown-topic" : value;
        return candidate.replaceAll("[^A-Za-z0-9_.=-]", "_");
    }

    static String sha256Hex(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(String.format("%02x", value));
            }
            return hex.toString();
        }
        catch (NoSuchAlgorithmException e) {
            throw new DebeziumException("SHA-256 is not available", e);
        }
    }

    record SerializedRecord(String key, byte[] payload, String sha256) {
    }
}
