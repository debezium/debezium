/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

/**
 * Represents a transaction begin event.
 *
 * @author Chris Cranford
 */
public class BeginEvent extends AbstractPayloadEvent {
    public BeginEvent() {
        super(Type.BEGIN);
    }
}
