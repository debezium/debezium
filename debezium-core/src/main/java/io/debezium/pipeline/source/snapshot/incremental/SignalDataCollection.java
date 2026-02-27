/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;

/**
 * A container class that holds both the signal payload and snapshot configuration for incremental snapshots.
 */
public class SignalDataCollection {
    private final SignalPayload signalPayload;
    private final SnapshotConfiguration snapshotConfiguration;

    public SignalDataCollection(SignalPayload signalPayload, SnapshotConfiguration snapshotConfiguration) {
        this.signalPayload = signalPayload;
        this.snapshotConfiguration = snapshotConfiguration;
    }

    public SignalPayload getSignalPayload() {
        return signalPayload;
    }

    public SnapshotConfiguration getSnapshotConfiguration() {
        return snapshotConfiguration;
    }
}