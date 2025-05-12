/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.runtime.DebeziumManifest;

class DefaultManifestHandler implements ManifestHandler {
    private final AtomicReference<DebeziumManifest> manifest;
    private final DebeziumManifest.Connector connector;

    DefaultManifestHandler(DebeziumManifest.Connector connector) {
        this.connector = connector;
        this.manifest = new AtomicReference<>(new DebeziumManifest(this.connector,
                new DebeziumManifest.Status(DebeziumManifest.Status.State.CREATING)));
    }

    @Override
    public ConnectorCallback connectorCallback() {
        return new ConnectorCallback() {
            public void pollingStarted() {
                manifest.set(new DebeziumManifest(connector, new DebeziumManifest.Status(DebeziumManifest.Status.State.POLLING)));
            }
        };
    }

    @Override
    public CompletionCallback completionCallback() {
        return (success, message, error) -> manifest.set(new DebeziumManifest(connector,
                new DebeziumManifest.Status(DebeziumManifest.Status.State.STOPPED)));
    }

    @Override
    public DebeziumManifest get() {
        return this.manifest.get();
    }

}
