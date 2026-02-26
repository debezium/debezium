/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

public class WhenNeededNoDataSnapshotter extends WhenNeededSnapshotter {

    @Override
    public String name() {
        return "when_needed_no_data";
    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
        return false;
    }
}