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
     * This interface is used to convert the default value literal to a Java type
     * recognized by value converters for a subset of types.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param defaultValueExpression the default value literal; may be null
     * @return value converted to a Java type; optional
     */
    Optional<Object> parseDefaultValue(Column column, String defaultValueExpression);

    /**
     * Obtain a DefaultValueConverter that passes through values.
     *
     * @return the pass-through DefaultValueConverter; never null
     */
    static DefaultValueConverter passthrough() {
        return (column, defaultValueExpression) -> Optional.ofNullable(defaultValueExpression);
    }

    /**
     * Converts the raw JDBC default value expression for a column into an object.
     */
    @FunctionalInterface
    public interface DefaultValueMapper {
        /**
         * Parses the string-representation of the default value to an object.
         *
         * @param column the column, should never be {@code null}
         * @param value the default value string representation / expression
         * @return the default value
         * @throws Exception if there was a parsing error
         */
        Object parse(Column column, String value) throws Exception;
    }
}
