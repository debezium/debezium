/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import java.util.Map;

/**
 * A pluggable strategy contract for defining how column names are resolved from kafka fields.
 *
 * @author Chris Cranford
 */
public interface ColumnNamingStrategy {
    /**
     * Resolves the logical field name from the change event to a column name.
     *
     * @param fieldName the field name, should not be {@code null}.
     * @return the resolved logical column name, never {@code null}.
     */
    String resolveColumnName(String fieldName);

    /**
     * Configures the strategy with additional properties.
     * This method is optional and can be overridden by implementations if configuration is needed.
     *
     * @param properties a map of properties to configure the strategy; never {@code null}.
     */
    default void configure(Map<String, String> properties) {
        // Default implementation does nothing
    }

}
