/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.debezium.util.Strings;

/**
 * Utility methods for encoding and decoding comma-separated enumerated value lists.
 */
public final class EnumeratedValues {

    private EnumeratedValues() {
    }

    public static String toCommaSeparatedString(List<String> values) {
        if (values == null) {
            return "";
        }
        return Strings.join(",", values.stream()
                .map(EnumeratedValues::escape)
                .toList());
    }

    public static List<String> fromCommaSeparatedString(String values) {
        if (values == null) {
            return Collections.emptyList();
        }
        final List<String> result = new ArrayList<>();
        final StringBuilder value = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i != values.length(); ++i) {
            final char c = values.charAt(i);
            if (escaped) {
                if (c == ',' || c == '\\') {
                    value.append(c);
                }
                else {
                    value.append('\\').append(c);
                }
                escaped = false;
            }
            else if (c == '\\') {
                escaped = true;
            }
            else if (c == ',') {
                result.add(value.toString());
                value.setLength(0);
            }
            else {
                value.append(c);
            }
        }
        if (escaped) {
            value.append('\\');
        }
        result.add(value.toString());
        return result;
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace(",", "\\,");
    }
}
