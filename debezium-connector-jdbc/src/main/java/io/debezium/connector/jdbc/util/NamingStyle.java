/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.util.stream.Stream;

import io.debezium.DebeziumException;

/**
 * Enum representing different naming styles for transforming string values.
 * Supported styles include snake_case, camelCase, upper_case, lower_case, and default (no transformation).
 *
 * @author Gustavo Lira
 */
public enum NamingStyle {
    SNAKE_CASE("snake_case"),
    CAMEL_CASE("camel_case"),
    UPPER_CASE("upper_case"),
    LOWER_CASE("lower_case"),
    DEFAULT("default");

    private final String value;

    NamingStyle(String value) {
        this.value = value;
    }

    /**
     * Retrieves a {@link NamingStyle} from its string representation.
     *
     * @param value the string representation of the naming style
     * @return the corresponding {@link NamingStyle}
     * @throws IllegalArgumentException if the value does not match any naming style
     */
    public static NamingStyle from(String value) {
        return Stream.of(values())
                .filter(style -> style.value.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new DebeziumException(
                        "Invalid naming style: " + value + ". Allowed styles are: " + String.join(", ", valuesAsString())));
    }

    /**
     * Retrieves the string value associated with this naming style.
     *
     * @return the string value of the naming style
     */
    public String getValue() {
        return value;
    }

    /**
     * Retrieves all available naming styles as a string array.
     *
     * @return an array of string values representing all naming styles
     */
    public static String[] valuesAsString() {
        return Stream.of(values())
                .map(NamingStyle::getValue)
                .toArray(String[]::new);
    }

}
