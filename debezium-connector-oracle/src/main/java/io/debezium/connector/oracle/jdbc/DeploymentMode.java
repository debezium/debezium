/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import io.debezium.config.EnumeratedValue;

/**
 * Defines the different type of Oracle deployment topology modes that are supported
 *
 * @author Chris Cranford
 */
public enum DeploymentMode implements EnumeratedValue {
    STANDARD("standard"),
    PHYSICAL_STANDBY("physical_standby");

    private final String value;

    DeploymentMode(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public static DeploymentMode parse(String value) {
        for (DeploymentMode mode : DeploymentMode.values()) {
            if (mode.getValue().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        return null;
    }
}
