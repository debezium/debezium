/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational;

import java.util.Optional;

/**
 * This interface is used to convert the string default value to a Java type
 * recognized by value converters for a subset of types.
 *
 * @author Jiabao Sun
 */
@FunctionalInterface
public interface DefaultValueConverter {

    /**
     * This interface is used to convert the string default value to a Java type
     * recognized by value converters for a subset of types.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param defaultValue the default value; may be null
     * @return value converted to a Java type; optional
     */
    Optional<Object> parseDefaultValue(Column column, String defaultValue);

    /**
     * Parse default value expression and set column's default value.
     * @param columnEditor
     * @return
     */
    default ColumnEditor setColumnDefaultValue(ColumnEditor columnEditor) {
        Column column = columnEditor.create();
        return parseDefaultValue(column, column.defaultValueExpression())
                .map(defaultValue -> column.edit().defaultValue(defaultValue))
                .orElse(column.edit());
    }

}
