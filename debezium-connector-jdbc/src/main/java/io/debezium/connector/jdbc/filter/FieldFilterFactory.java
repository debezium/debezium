/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.filter;

import io.debezium.util.Strings;

/**
 * A generalized {@link FieldFilterFactory} implementation.
 *
 * @author Anisha Mohanty
 */
public class FieldFilterFactory {

    @FunctionalInterface
    public interface FieldNameFilter {
        boolean matches(String topicName, String columnName);

    }

    /** Default filter that always includes a field */
    public static FieldNameFilter DEFAULT_FILTER = (topicName, columnName) -> true;

    private static FieldNameFilter createFilter(String fieldList, boolean include) {
        String[] entries = fieldList.split(",");

        return (topicName, fieldName) -> {
            for (String entry : entries) {
                String[] parts = entry.split(":");
                if (parts.length == 2 && parts[0].equals(topicName) && parts[1].equals(fieldName)) {
                    return include;
                }
            }
            return !include;
        };
    }

    private static FieldNameFilter createIncludeFilter(String fieldIncludeList) {
        return createFilter(fieldIncludeList, true);
    }

    private static FieldNameFilter createExcludeFilter(String fieldExcludeList) {
        return createFilter(fieldExcludeList, false);
    }

    public static FieldNameFilter createFieldFilter(String includeList, String excludeList) {
        if (!Strings.isNullOrEmpty(excludeList)) {
            return createExcludeFilter(excludeList);
        }
        else if (!Strings.isNullOrEmpty(includeList)) {
            return createIncludeFilter(includeList);
        }
        else {
            // Always match and include as no filters were specified.
            return DEFAULT_FILTER;
        }
    }
}
