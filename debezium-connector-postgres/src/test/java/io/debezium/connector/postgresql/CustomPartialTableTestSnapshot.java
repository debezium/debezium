/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.List;
import java.util.Optional;

import io.debezium.spi.snapshot.Snapshotter;

public class CustomPartialTableTestSnapshot extends CustomStartFromStreamingTestSnapshot implements Snapshotter {

    @Override
    public String name() {
        return CustomPartialTableTestSnapshot.class.getName();
    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        if (tableId.contains("s1") && tableId.contains("a")) {
            return super.snapshotQuery(tableId, snapshotSelectColumns);
        }

        return Optional.empty();
    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshot() {
        return true;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnDataError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotSchema() {
        return false;
    }

    @Override
    public boolean shouldStreamEventsStartingFromSnapshot() {
        return false;
    }
}
