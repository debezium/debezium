/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.EnumeratedValue;

/**
 * Reaction of {@link DecisionToHeaders} to a failed evaluation.
 *
 * @author Jiri Pechanec
 */
public enum ErrorMode implements EnumeratedValue {

    /** Throw an exception, the task fails. */
    FAIL("fail"),
    /** Log a warning and pass the record unchanged. */
    SKIP("skip"),
    /** Log a warning and mark the record with status and error headers. */
    HEADER("header");

    private final String value;

    ErrorMode(final String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public static ErrorMode parse(final String value) {
        if (value == null || value.isBlank()) {
            return FAIL;
        }
        for (final var mode : values()) {
            if (mode.value.equalsIgnoreCase(value.trim())) {
                return mode;
            }
        }
        throw new ConfigException("Invalid error mode '" + value + "', use one of fail, skip, header");
    }
}
