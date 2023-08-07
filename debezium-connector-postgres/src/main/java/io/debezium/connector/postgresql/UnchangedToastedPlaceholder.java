/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public UnchangedToastedPlaceholder(PostgresConnectorConfig connectorConfig) {
        toastPlaceholderBinary = connectorConfig.getUnavailableValuePlaceholder();
        toastPlaceholderString = new String(toastPlaceholderBinary);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE, toastPlaceholderString);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_TEXT_ARRAY_TOAST_VALUE,
                Arrays.asList(toastPlaceholderString));
        final List<Integer> toastedIntArrayPlaceholder = new ArrayList<>(toastPlaceholderBinary.length);
        final List<Long> toastedLongArrayPlaceholder = new ArrayList<>(toastPlaceholderBinary.length);
        for (byte b : toastPlaceholderBinary) {
            toastedIntArrayPlaceholder.add((int) b);
            toastedLongArrayPlaceholder.add((long) b);
        }
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_INT_ARRAY_TOAST_VALUE, toastedIntArrayPlaceholder);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_BIGINT_ARRAY_TOAST_VALUE, toastedLongArrayPlaceholder);
        toastPlaceholderHstore.put(toastPlaceholderString, toastPlaceholderString);
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_HSTORE_TOAST_VALUE, toastPlaceholderHstore);
    }

    public Optional<Object> getValue(Object obj) {
        return Optional.ofNullable(placeholderValues.get(obj));
    }

    public byte[] getToastPlaceholderBinary() {
        return toastPlaceholderBinary;
    }

    public String getToastPlaceholderString() {
        return toastPlaceholderString;
    }
}
