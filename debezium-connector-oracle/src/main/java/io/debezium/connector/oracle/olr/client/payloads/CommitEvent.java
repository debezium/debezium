/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

/**
 * Represents a transaction commit event.
 *
 * @author Chris Cranford
 */
public class CommitEvent extends AbstractPayloadEvent {
    public CommitEvent() {
        super(Type.COMMIT);
    }
}
