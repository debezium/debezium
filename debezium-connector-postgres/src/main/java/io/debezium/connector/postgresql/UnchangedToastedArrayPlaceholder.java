/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnchangedToastedArrayPlaceholder {

    private final Map<Object, Object> placeholderValues = new HashMap<Object, Object>();

    public UnchangedToastedArrayPlaceholder(PostgresConnectorConfig connectorConfig) {
        String toastPlaceholderString = new String(connectorConfig.getUnavailableValuePlaceholder());
        String toastPlaceholderNumberArrayString = connectorConfig.getUnavailableNumberArrayPlaceholder();
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_TEXT_ARRAY_TOAST_VALUE, Arrays.asList(toastPlaceholderString));
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_INT_ARRAY_TOAST_VALUE,
                Stream.of(toastPlaceholderNumberArrayString.split(","))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList()));
        placeholderValues.put(UnchangedToastedReplicationMessageColumn.UNCHANGED_BIGINT_ARRAY_TOAST_VALUE,
                Stream.of(toastPlaceholderNumberArrayString.split(","))
                        .map(String::trim)
                        .map(Long::parseLong)
                        .collect(Collectors.toList()));
    }

    public Object getValue(Object obj) {
        return placeholderValues.get(obj);
    }
}
