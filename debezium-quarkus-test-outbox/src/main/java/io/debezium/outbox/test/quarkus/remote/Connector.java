/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.util.Objects;

/**
 * Represents a Connector with a state.
 */
public record Connector(String state) {
    /**
     * Constructor that requires the state to be non-null.
     *
     * @param state the state of the connector
     * @throws NullPointerException if the state is null
     */
    public Connector {
        Objects.requireNonNull(state);
    }
}
