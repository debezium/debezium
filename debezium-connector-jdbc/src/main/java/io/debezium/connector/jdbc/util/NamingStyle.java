/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.util.stream.Stream;

import io.debezium.DebeziumException;
import io.debezium.config.EnumeratedValue;

/**
 * Enum representing different naming styles for transforming string values.
 * Supported styles include snake_case, camelCase, UPPER_CASE, lower_case, and default (no transformation).
 *
 * @author Gustavo Lira
 */
public enum NamingStyle implements EnumeratedValue {
    SNAKE_CASE("snake_case"),
    CAMEL_CASE("camel_case"),
    UPPER_CASE("UPPER_CASE"),
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
     * @throws DebeziumException if the value does not match any naming style
     */
    public static NamingStyle from(String value) {
        NamingStyle style = EnumeratedValue.parse(NamingStyle.class, value);
        if (style != null) {
            return style;
        }
        return Stream.of(values())
                .filter(option -> option.name().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new DebeziumException(
                        "Invalid naming style: " + value + ". Allowed styles are: " + String.join(", ", valuesAsString())));
    }

    /**
     * Retrieves the string value associated with this naming style.
     *
     * @return the string value of the naming style
     */
    @Override
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
                .map(NamingStyle::name)
                .toArray(String[]::new);
    }
}
