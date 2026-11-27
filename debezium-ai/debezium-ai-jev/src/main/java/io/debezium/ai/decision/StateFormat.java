/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.EnumeratedValue;

/**
 * Form of the state sent to the {@link io.debezium.ai.decision.api.DecisionService}.
 *
 * @author Jiri Pechanec
 */
public enum StateFormat implements EnumeratedValue {

    /** JSON object keyed by the configured field paths. */
    JSON("json"),
    /** Non-null values converted to strings and joined by a new line. */
    TEXT("text");

    private final String value;

    StateFormat(final String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public static StateFormat parse(final String value) {
        if (value == null || value.isBlank()) {
            return JSON;
        }
        for (final var format : values()) {
            if (format.value.equalsIgnoreCase(value.trim())) {
                return format;
            }
        }
        throw new ConfigException("Invalid state format '" + value + "', use one of json, text");
    }
}
