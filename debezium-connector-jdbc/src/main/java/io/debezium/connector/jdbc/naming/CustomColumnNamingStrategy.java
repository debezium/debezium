/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import java.util.Map;

import io.debezium.connector.jdbc.util.AbstractNamingStrategy;

/**
 * A custom implementation of the {@link io.debezium.sink.naming.ColumnNamingStrategy} interface
 * that supports various naming styles and customizations such as snake_case,
 * camelCase, and prefix/suffix additions.
 *
 * @author Gustavo Lira
 */
public class CustomColumnNamingStrategy extends AbstractNamingStrategy implements ColumnNamingStrategy {

    private static final String PREFIX_KEY = "column.naming.prefix";
    private static final String SUFFIX_KEY = "column.naming.suffix";
    private static final String STYLE_KEY = "column.naming.style";

    /**
     * Resolves the column name by applying the configured naming style, prefix, and suffix.
     *
     * @param fieldName the original field name to transform
     * @return the transformed column name
     */
    @Override
    public String resolveColumnName(String fieldName) {
        return applyNaming(fieldName);
    }

    /**
     * Configures the naming strategy using the provided properties map.
     *
     * @param properties the map of configuration properties
     */
    @Override
    public void configure(Map<String, String> properties) {
        configure(properties, PREFIX_KEY, SUFFIX_KEY, STYLE_KEY);
    }
}
