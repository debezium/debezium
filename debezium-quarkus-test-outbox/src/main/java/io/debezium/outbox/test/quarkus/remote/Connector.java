/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.util.Objects;

/**
 * Represents a Connector with a state.
 *
 * <p>
 * This class encapsulates the state of a connector and provides a method to check
 * if the connector is currently running. The connector state is expected to be a
 * non-null string that indicates the current operational state of the connector.
 * </p>
 *
 * <p>
 * The class includes a constructor that enforces the requirement that the state
 * cannot be null. It also provides a method {@link #isRunning()} to check if the
 * connector is in the running state.
 * </p>
 */public record Connector(String state) {
    private static final String RUNNING_STATE = "RUNNING";

    /**
     * Constructor that requires the state to be non-null.
     *
     * @param state the state of the connector
     * @throws NullPointerException if the state is null
     */
    public Connector {
        Objects.requireNonNull(state);
    }

    /**
     * Checks if the connector is currently in the running state.
     *
     * @return true if the connector state is "RUNNING", false otherwise
     */
    public Boolean isRunning() {
        return RUNNING_STATE.equals(state);
    }

}
