/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.IntFunction;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;

/**
 * Helper that returns placeholder values for unchanged toasted columns.
 *
 * The configured placeholder is converted to a data type that is compatible with the given column type.
 *
 * @author Praveen Burgu
 */
public class UnchangedToastedPlaceholder {

    private final Map<Object, Object> placeholderValues = new HashMap<Object, Object>();
    private final byte[] toastPlaceholderBinary;
    private final String toastPlaceholderString;
    private final Map<String, String> toastPlaceholderHstore = new HashMap<>();
    private final String toastPlaceholderUuid;

    /**
     * Provides different representations of a placeholder value.<br>
     *
     * <b>NOTE:</b> Adding new types might require an update in {@link io.debezium.processors.reselect.ReselectColumnsPostProcessor}.
     *
     * @param connectorConfig
     */
    public UnchangedToastedPlaceholder(PostgresConnectorConfig connectorConfig) {
        toastPlaceholderBinary = connectorConfig.getUnavailableValuePlaceholder();
        toastPlaceholderString = new String(toastPlaceholderBinary);
        toastPlaceholderUuid = UUID.nameUUIDFromBytes(toastPlaceholderBinary).toString();
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE, toastPlaceholderString);
        toastPlaceholderHstore.put(toastPlaceholderString, toastPlaceholderString);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_HSTORE_TOAST_VALUE, toastPlaceholderHstore);
    }

    public Optional<Object> getValue(Object obj) {
        return Optional.ofNullable(placeholderValues.get(obj));
    }

    /**
     * Returns the placeholder for an array column, expressed in the array's element type.
     * <p>
     * Text-like elements carry the placeholder as a single element, every other element type carries one
     * element per placeholder byte, which is how the {@code integer[]} and {@code bigint[]} placeholders
     * have always been built. An empty result means the element type has no placeholder representation
     * yet, in which case the caller keeps the existing behaviour.
     *
     * @param elementSchema schema of the array's elements; never null
     */
    public Optional<List<Object>> getArrayValue(Schema elementSchema) {
        switch (elementSchema.type()) {
            case STRING:
                return Optional.of(List.of(Uuid.LOGICAL_NAME.equals(elementSchema.name()) ? toastPlaceholderUuid : toastPlaceholderString));
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(elementSchema.name())) {
                    return Optional.of(placeholderBytesAs(b -> Decimal.toLogical(elementSchema, new byte[]{ (byte) b })));
                }
                return Optional.of(List.of(toastPlaceholderBinary));
            case INT16:
                return Optional.of(placeholderBytesAs(b -> (short) b));
            case INT32:
                return Optional.of(placeholderBytesAs(b -> b));
            case INT64:
                return Optional.of(placeholderBytesAs(b -> (long) b));
            case FLOAT32:
                return Optional.of(placeholderBytesAs(b -> (float) b));
            case FLOAT64:
                return Optional.of(placeholderBytesAs(b -> (double) b));
            case BOOLEAN:
                return Optional.of(placeholderBytesAs(b -> b != 0));
            case STRUCT:
                if (VariableScaleDecimal.LOGICAL_NAME.equals(elementSchema.name())) {
                    return Optional.of(placeholderBytesAs(b -> VariableScaleDecimal.fromLogical(elementSchema, BigDecimal.valueOf(b))));
                }
                return Optional.empty();
            default:
                return Optional.empty();
        }
    }

    private List<Object> placeholderBytesAs(IntFunction<Object> elementMapper) {
        final List<Object> placeholder = new ArrayList<>(toastPlaceholderBinary.length);
        for (byte b : toastPlaceholderBinary) {
            placeholder.add(elementMapper.apply(b));
        }
        return placeholder;
    }

    public byte[] getToastPlaceholderBinary() {
        return toastPlaceholderBinary;
    }

    public String getToastPlaceholderString() {
        return toastPlaceholderString;
    }
}
