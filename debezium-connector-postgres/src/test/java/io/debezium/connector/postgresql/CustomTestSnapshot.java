/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;

import java.util.Optional;

/**
 * This is a small class used in PostgresConnectorIT to test a custom snapshot
 *
 * It is tightly coupled to the test there, but needs to be placed here in order
 * to allow for class loading to work
 */
public class CustomTestSnapshot implements Snapshotter {
    private boolean hasState;
    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        hasState = (sourceInfo != null);
    }

    @Override
    public boolean shouldSnapshot() {
        return true;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId) {
        // on an empty state, don't read from s2 schema, but afterwards, do
        if (!hasState && tableId.schema().equals("s2")) {
            return Optional.empty();
        }
        else {
            return Optional.of("select * from " + tableId.toDoubleQuotedString());
        }
    }
}
