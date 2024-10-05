/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.util.Objects;

/**
 * Represents the status of a Debezium connector.
 */
public record DebeziumConnectorStatus(String name, Connector connector) {
    private static final String RUNNING_STATE = "RUNNING";

    /**
     * Constructor that requires name and connector to be non-null.
     *
     * @param name the name of the connector
     * @param connector the connector object containing state
     * @throws NullPointerException if name or connector is null
     */
    public DebeziumConnectorStatus {
        Objects.requireNonNull(name);
        Objects.requireNonNull(connector);
    }

    /**
     * Checks if the connector is in the RUNNING state.
     *
     * @return true if the connector state is RUNNING, false otherwise
     */
    public Boolean isRunning() {
        return RUNNING_STATE.equals(this.connector.state());
    }

}
