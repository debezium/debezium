/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

/**
 * Schema parameter that declares the exact unavailable value placeholder representation a connector
 * emits for a column, so that consumers such as the {@code ReselectColumnsPostProcessor} can recognize
 * placeholder values by comparing against the declared representation instead of inferring it from the
 * value shape.
 *
 * <p>The parameter value is the {@link #serialize(Schema, Object) canonical serialization} of the
 * placeholder value as it appears in emitted change events. Producers attach the parameter with the
 * serialization of the placeholder they emit; consumers compare a field value against it with
 * {@link #matches(Schema, Object, String)}.
 *
 * @author Sundong Kim
 */
public class UnavailableValuePlaceholderParameter {

    /**
     * Key of the schema parameter carrying the serialized placeholder representation.
     */
    public static final String SCHEMA_PARAMETER_KEY = "__debezium.unavailable.value.placeholder";

    private UnavailableValuePlaceholderParameter() {
    }

    /**
     * Determines whether the given value is the placeholder declared by the given canonical
     * representation. The comparison work is bounded by the length of the declared representation,
     * so values much larger than the placeholder are rejected without being serialized in full.
     *
     * @param schema the schema of the field holding the value; may be null
     * @param value the value to compare; may be null
     * @param declared the declared canonical placeholder representation; must not be null
     * @return {@code true} if the value is the declared placeholder
     */
    public static boolean matches(Schema schema, Object value, String declared) {
        return declared.equals(serialize(schema, value, declared.length()));
    }

    /**
     * Serializes the given value to the canonical string representation used by the
     * {@link #SCHEMA_PARAMETER_KEY} parameter.
     *
     * @param schema the schema of the field holding the value; may be null
     * @param value the value to serialize; may be null
     * @return the canonical representation, or {@code null} if the value cannot be represented
     */
    public static String serialize(Schema schema, Object value) {
        return serialize(schema, value, Integer.MAX_VALUE);
    }

    /**
     * A value whose representation would exceed {@code maxLength} is rejected with {@code null} rather
     * than truncated, so comparing against a shorter declared placeholder never serializes it in full.
     */
    private static String serialize(Schema schema, Object value, int maxLength) {
        if (schema == null || value == null) {
            return null;
        }
        switch (schema.type()) {
            case STRING:
                return value instanceof String string && string.length() <= maxLength ? string : null;
            case BYTES:
                return serializeBytes(value, maxLength);
            case INT32:
            case INT64:
                return value instanceof Number ? value.toString() : null;
            case ARRAY:
                return serializeArray(schema, value, maxLength);
            case MAP:
                return serializeMap(schema, value, maxLength);
            default:
                return null;
        }
    }

    private static String serializeBytes(Object value, int maxLength) {
        byte[] bytes;
        if (value instanceof byte[] byteArray) {
            bytes = byteArray;
        }
        else if (value instanceof ByteBuffer byteBuffer) {
            final ByteBuffer buffer = byteBuffer.duplicate();
            if (base64Length(buffer.remaining()) > maxLength) {
                return null;
            }
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        else {
            return null;
        }
        return base64Length(bytes.length) <= maxLength ? Base64.getEncoder().encodeToString(bytes) : null;
    }

    private static long base64Length(int byteCount) {
        return 4L * ((byteCount + 2L) / 3L);
    }

    private static String serializeArray(Schema schema, Object value, int maxLength) {
        if (!(value instanceof Collection)) {
            return null;
        }
        final StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Object element : (Collection<?>) value) {
            if (sb.length() > maxLength) {
                return null;
            }
            final String serialized = serializeElement(schema.valueSchema(), element, maxLength);
            if (serialized == null) {
                return null;
            }
            if (!first) {
                sb.append(',');
            }
            sb.append(serialized);
            first = false;
        }
        return sb.length() + 1 <= maxLength ? sb.append(']').toString() : null;
    }

    private static String serializeMap(Schema schema, Object value, int maxLength) {
        if (!(value instanceof Map)) {
            return null;
        }
        final StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            if (sb.length() > maxLength) {
                return null;
            }
            final String key = serializeElement(schema.keySchema(), entry.getKey(), maxLength);
            final String entryValue = serializeElement(schema.valueSchema(), entry.getValue(), maxLength);
            if (key == null || entryValue == null) {
                return null;
            }
            if (!first) {
                sb.append(',');
            }
            sb.append(key).append(':').append(entryValue);
            first = false;
        }
        return sb.length() + 1 <= maxLength ? sb.append('}').toString() : null;
    }

    /**
     * Serializes a nested value: strings and binary values are quoted and escaped so that
     * representations of composite values are unambiguous. Quoting only lengthens a value, so
     * bounding the unquoted length never rejects an element that would have fit.
     */
    private static String serializeElement(Schema schema, Object value, int maxLength) {
        if (schema == null || value == null) {
            return null;
        }
        switch (schema.type()) {
            case STRING:
                return value instanceof String string && string.length() <= maxLength ? quote(string) : null;
            case BYTES:
                final String bytes = serializeBytes(value, maxLength);
                return bytes != null ? quote(bytes) : null;
            default:
                return serialize(schema, value, maxLength);
        }
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
