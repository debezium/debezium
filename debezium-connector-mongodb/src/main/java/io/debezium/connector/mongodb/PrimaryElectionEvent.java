/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import com.mongodb.ServerAddress;

import io.debezium.pipeline.ConnectorEvent;

/**
 * An event that signals that a primary election has occurred.
 *
 * @author Chris Cranford
 */
public class PrimaryElectionEvent implements ConnectorEvent {

    private final ServerAddress primaryAddress;

    public PrimaryElectionEvent(ServerAddress primaryAddress) {
        this.primaryAddress = primaryAddress;
    }

    public ServerAddress getPrimaryAddress() {
        return primaryAddress;
    }
}
