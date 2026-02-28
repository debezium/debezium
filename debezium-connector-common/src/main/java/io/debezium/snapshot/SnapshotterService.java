/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.service.Service;
import io.debezium.snapshot.spi.SnapshotLock;
import io.debezium.snapshot.spi.SnapshotQuery;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * Registry of all available snapshotters.
 *
 * @author Mario Fiore Vitale
 */
@ThreadSafe
public class SnapshotterService implements Service {

    @Immutable
    private final Snapshotter snapshotter;
    @Immutable
    private final SnapshotQuery snapshotQuery;
    @Immutable
    private final SnapshotLock snapshotLock;

    public SnapshotterService(Snapshotter snapshotter, SnapshotQuery snapshotQuery, SnapshotLock snapshotLock) {

        this.snapshotter = snapshotter;
        this.snapshotQuery = snapshotQuery;
        this.snapshotLock = snapshotLock;
    }

    public SnapshotQuery getSnapshotQuery() {
        return snapshotQuery;
    }

    public SnapshotLock getSnapshotLock() {
        return snapshotLock;
    }

    public Snapshotter getSnapshotter() {
        return this.snapshotter;
    }
}
