/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.filter;

/**
 * A generalized {@link ColumnFilterFactory} implementation.
 *
 * @author Anisha Mohanty
 */

public class ColumnFilterFactory {

    @FunctionalInterface
    public interface ColumnNameFilter {
        boolean matches(String topicName, String columnName);

    }

    private static ColumnNameFilter createFilter(String columnList, boolean include) {
        String[] entries = columnList.split(",");

        return (topicName, columnName) -> {
            for (String entry : entries) {
                String[] parts = entry.split(":");
                if (parts.length == 2 && parts[0].equals(topicName) && parts[1].equals(columnName)) {
                    return include;
                }
            }
            return !include;
        };
    }

    public static ColumnNameFilter createIncludeFilter(String columnIncludeList) {
        return createFilter(columnIncludeList, true);
    }

    public static ColumnNameFilter createExcludeFilter(String columnExcludeList) {
        return createFilter(columnExcludeList, false);
    }
}
