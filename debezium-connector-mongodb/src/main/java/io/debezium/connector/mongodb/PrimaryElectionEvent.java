/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import com.mongodb.ServerAddress;

import io.debezium.pipeline.MetadataEvent;

/**
 * A metadata event that signals that a primary election event was detected.
 *
 * @author Chris Cranford
 */
public class PrimaryElectionEvent implements MetadataEvent {

    private final ServerAddress primaryAddress;

    public PrimaryElectionEvent(ServerAddress primaryAddress) {
        this.primaryAddress = primaryAddress;
    }

    public ServerAddress getPrimaryAddress() {
        return primaryAddress;
    }
}
