/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.producer;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.quarkus.debezium.engine.DebeziumManifest;
import io.quarkus.debezium.engine.DebeziumManifest.Connector;
import io.quarkus.debezium.engine.DebeziumManifest.Status;
import io.quarkus.debezium.engine.ManifestHandler;

class DefaultManifestHandler implements ManifestHandler {
    private final AtomicReference<DebeziumManifest> manifest;
    private final Connector connector;

    protected DefaultManifestHandler(Connector connector) {
        this.connector = connector;
        this.manifest = new AtomicReference<>(new DebeziumManifest(this.connector,
                new Status(Status.State.CREATING)));
    }

    @Override
    public ConnectorCallback connectorCallback() {
        return new ConnectorCallback() {
            public void pollingStarted() {
                manifest.set(new DebeziumManifest(connector, new Status(Status.State.POLLING)));
            }
        };
    }

    @Override
    public CompletionCallback completionCallback() {
        return (success, message, error) -> manifest.set(new DebeziumManifest(connector,
                new Status(Status.State.STOPPED)));
    }

    @Override
    public DebeziumManifest get() {
        return this.manifest.get();
    }

}
