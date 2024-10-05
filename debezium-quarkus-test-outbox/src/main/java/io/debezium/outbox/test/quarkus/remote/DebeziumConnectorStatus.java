/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.util.Objects;

/**
 * Represents the status of a Debezium connector.
 *
 * <p>
 * This class encapsulates the status of a Debezium connector by holding its name
 * and an instance of the {@link io.debezium.outbox.test.quarkus.remote.Connector} class, which indicates the current
 * operational state of the connector. It provides a method to check if the
 * connector is running based on its internal state.
 * </p>
 *
 * <p>
 * The constructor enforces that both the name and the connector must be non-null.
 * It also includes a method {@link #isRunning()} to determine if the connector
 * is currently in the running state.
 * </p>
 */public record DebeziumConnectorStatus(String name, Connector connector) {

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
     * Checks if the connector is currently in the running state.
     *
     * @return true if the connector is running, false otherwise
     */
    public Boolean isRunning() {
        return this.connector.isRunning();
    }

}
