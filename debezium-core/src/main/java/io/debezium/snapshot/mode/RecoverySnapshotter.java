/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

public class RecoverySnapshotter extends NoDataSnapshotter {

    @Override
    public String name() {
        return "recovery";
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
        return true;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return true;
    }

}
