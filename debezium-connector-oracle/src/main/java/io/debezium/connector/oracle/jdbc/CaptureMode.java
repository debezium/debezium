/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import io.debezium.config.EnumeratedValue;

/**
 * Defines the different type of Oracle streaming capture modes that are supported
 *
 * @author Chris Cranford
 */
public enum CaptureMode implements EnumeratedValue {
    /**
     * Captures changes from the primary Oracle instance.
     */
    PRIMARY("primary"),

    /**
     * Captures changes from a physical standby instance.
     */
    PHYSICAL_STANDBY("physical_standby"),

    /**
     * Downstream mining instance.
     */
    DOWNSTREAM("downstream");

    private final String value;

    CaptureMode(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public static CaptureMode parse(String value) {
        for (CaptureMode mode : CaptureMode.values()) {
            if (mode.getValue().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        return null;
    }
}
