/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.relational.TableId;

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
    public Optional<String> buildSnapshotQuery(TableId tableId, List<String> snapshotSelectColumns) {
        if (!hasState && tableId.schema().equals("s2")) {
            return Optional.empty();
        }
        else {
            String query = snapshotSelectColumns.stream()
                    .collect(Collectors.joining(", ", "SELECT ", " FROM ONLY " + tableId.toDoubleQuotedString()));

            return Optional.of(query);
        }
    }

    @Override
    public String snapshotTransactionIsolationLevelStatement(SlotCreationResult newSlotInfo, boolean isOnDemand) {
        if (newSlotInfo != null) {
            String snapSet = String.format("SET TRANSACTION SNAPSHOT '%s';", newSlotInfo.snapshotName());
            return "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \n" + snapSet;
        }
        else {
            return "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;";
        }
    }
}
