/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.model.debezium;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Property validation rule.
 * <p>
 * Use factory methods to create type-specific validations:
 * <ul>
 *   <li>{@link #regex(String)} - Regex pattern validation</li>
 *   <li>{@link #enumValues(List)} - Enum/allowed values validation</li>
 *   <li>{@link #min(Number)} - Minimum value validation</li>
 *   <li>{@link #max(Number)} - Maximum value validation</li>
 *   <li>{@link #range(Number, Number)} - Range validation (min and max)</li>
 * </ul>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Validation(
        @JsonProperty("type") String type,
        @JsonProperty("pattern") String pattern,
        @JsonProperty("message") String message,
        @JsonProperty("min") Number min,
        @JsonProperty("max") Number max,
        @JsonProperty("values") List<String> values) {

    /**
     * Creates a regex validation rule.
     *
     * @param pattern the regex pattern to validate against
     * @return validation rule with type "regex"
     */
    public static Validation regex(String pattern) {
        return new Validation("regex", pattern, null, null, null, null);
    }

    /**
     * Creates a regex validation rule with custom error message.
     *
     * @param pattern the regex pattern to validate against
     * @param message custom error message
     * @return validation rule with type "regex"
     */
    public static Validation regex(String pattern, String message) {
        return new Validation("regex", pattern, message, null, null, null);
    }

    /**
     * Creates an enum validation rule with allowed values.
     *
     * @param values the list of allowed values
     * @return validation rule with type "enum"
     */
    public static Validation enumValues(List<String> values) {
        return new Validation("enum", null, null, null, null, values);
    }

    /**
     * Creates an enum validation rule with allowed values and custom error message.
     *
     * @param values the list of allowed values
     * @param message custom error message
     * @return validation rule with type "enum"
     */
    public static Validation enumValues(List<String> values, String message) {
        return new Validation("enum", null, message, null, null, values);
    }

    /**
     * Creates a minimum value validation rule.
     *
     * @param min the minimum acceptable value
     * @return validation rule with type "min"
     */
    public static Validation min(Number min) {
        return new Validation("min", null, null, min, null, null);
    }

    /**
     * Creates a minimum value validation rule with custom error message.
     *
     * @param min the minimum acceptable value
     * @param message custom error message
     * @return validation rule with type "min"
     */
    public static Validation min(Number min, String message) {
        return new Validation("min", null, message, min, null, null);
    }

    /**
     * Creates a maximum value validation rule.
     *
     * @param max the maximum acceptable value
     * @return validation rule with type "max"
     */
    public static Validation max(Number max) {
        return new Validation("max", null, null, null, max, null);
    }

    /**
     * Creates a maximum value validation rule with custom error message.
     *
     * @param max the maximum acceptable value
     * @param message custom error message
     * @return validation rule with type "max"
     */
    public static Validation max(Number max, String message) {
        return new Validation("max", null, message, null, max, null);
    }

    /**
     * Creates a range validation rule with both minimum and maximum values.
     *
     * @param min the minimum acceptable value
     * @param max the maximum acceptable value
     * @return validation rule with type "range"
     */
    public static Validation range(Number min, Number max) {
        return new Validation("range", null, null, min, max, null);
    }

    /**
     * Creates a range validation rule with both minimum and maximum values and custom error message.
     *
     * @param min the minimum acceptable value
     * @param max the maximum acceptable value
     * @param message custom error message
     * @return validation rule with type "range"
     */
    public static Validation range(Number min, Number max, String message) {
        return new Validation("range", null, message, min, max, null);
    }
}
