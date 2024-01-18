/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Map;

import io.debezium.DebeziumException;

public final class OffsetUtils {

    public static long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString());
        }
        catch (NumberFormatException e) {
            throw new DebeziumException("Source offset '" + key + "' parameter value " + obj + " could not be converted to a long");
        }
    }

    public static String stringOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return null;
        }
        return (String) obj;
    }

    public static boolean booleanOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }
        return false;
    }

    private OffsetUtils() {
        // intentionally private
    }
}
