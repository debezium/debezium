/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.Objects;

/**
 * A configuration option with a fixed set of possible values, i.e. an enum. To be implemented by any enum
 * types used with {@link ConfigBuilder}.
 *
 * @author Brendan Maguire
 */
public interface EnumeratedValue {

    /**
     * Returns the string representation of this value
     * @return The string representation of this value
     */
    String getValue();

    /**
     * Determine whether this enumerated option matches the supplied value.
     *
     * @param value the value to compare; may be null
     * @return true when the value matches this option
     */
    default boolean matches(String value) {
        return value != null && getValue().equalsIgnoreCase(value.trim());
    }

    /**
     * Parse the supplied value into the matching enum constant.
     *
     * @param enumType the enum type to parse
     * @param value the supplied value
     * @param <T> enum type
     * @return the matching enum value
     */
    static <T extends Enum<T> & EnumeratedValue> T parse(Class<T> enumType, String value) {
        Objects.requireNonNull(enumType, "enumType must not be null");
        if (value == null) {
            return null;
        }
        for (T option : enumType.getEnumConstants()) {
            if (option.matches(value)) {
                return option;
            }
        }
        return null;
    }

    /**
     * Parse the supplied value into the matching enum constant, falling back to the supplied default value.
     *
     * @param enumType the enum type to parse
     * @param value the supplied value
     * @param defaultValue the default value string
     * @param <T> enum type
     * @return the matching enum value
     * @throws IllegalArgumentException if no match can be found
     */
    static <T extends Enum<T> & EnumeratedValue> T parse(Class<T> enumType, String value, String defaultValue) {
        T mode = parse(enumType, value);
        if (mode == null && defaultValue != null) {
            mode = parse(enumType, defaultValue);
        }
        if (mode == null) {
            throw new IllegalArgumentException("Unknown value '" + value + "' for enum " + enumType.getName());
        }
        return mode;
    }
}
