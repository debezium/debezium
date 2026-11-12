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
import java.util.Set;
import java.util.UUID;
import java.util.function.IntFunction;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

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

    private static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";
    private static final Set<String> CONNECT_TEMPORAL_NAMES = Set.of(Date.LOGICAL_NAME, Time.LOGICAL_NAME, Timestamp.LOGICAL_NAME);

    private final Map<Object, Object> placeholderValues = new HashMap<Object, Object>();
    private final byte[] toastPlaceholderBinary;
    private final String toastPlaceholderString;
    private final Map<String, String> toastPlaceholderHstore = new HashMap<>();
    private final String toastPlaceholderUuid;

    /**
     * Provides different representations of a placeholder value.
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
     * Text-like and binary elements carry the placeholder as a single element, numeric elements carry one
     * element per placeholder byte. An empty result means the element type has no placeholder representation;
     * boolean elements have none because a list of per-byte booleans cannot be told apart from data (see debezium/dbz#1601).
     * <p>
     * <b>NOTE:</b> a new element type has to be recognized by
     * {@link io.debezium.processors.reselect.ReselectColumnsPostProcessor} as well, otherwise the column
     * is no longer re-selected.
     *
     * @param elementSchema schema of the array's elements; never null
     */
    public Optional<List<Object>> getArrayValue(Schema elementSchema) {
        if (elementSchema.name() != null && CONNECT_TEMPORAL_NAMES.contains(elementSchema.name())) {
            // time.precision.mode=connect elements are java.util.Date, which a placeholder byte cannot express
            return Optional.empty();
        }
        switch (elementSchema.type()) {
            case STRING:
                return Optional.of(List.of(Uuid.LOGICAL_NAME.equals(elementSchema.name()) ? toastPlaceholderUuid : toastPlaceholderString));
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(elementSchema.name())) {
                    final int scale = Integer.parseInt(elementSchema.parameters().get(Decimal.SCALE_FIELD));
                    // a placeholder that does not fit the declared precision would be rejected by consumers of the column type
                    final String precision = elementSchema.parameters().get(PRECISION_PARAMETER_KEY);
                    if (precision != null && placeholderMaxAbs() >= Math.pow(10, Integer.parseInt(precision) - scale)) {
                        return Optional.empty();
                    }
                    return Optional.of(placeholderBytesAs(b -> BigDecimal.valueOf(b).setScale(scale)));
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
            case STRUCT:
                if (VariableScaleDecimal.LOGICAL_NAME.equals(elementSchema.name())) {
                    return Optional.of(placeholderBytesAs(b -> VariableScaleDecimal.fromLogical(elementSchema, BigDecimal.valueOf(b))));
                }
                return Optional.empty();
            default:
                return Optional.empty();
        }
    }

    private int placeholderMaxAbs() {
        int max = 0;
        for (byte b : toastPlaceholderBinary) {
            max = Math.max(max, Math.abs(b));
        }
        return max;
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
