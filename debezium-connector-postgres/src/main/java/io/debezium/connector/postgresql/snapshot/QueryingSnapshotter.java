/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.relational.TableId;

public abstract class QueryingSnapshotter implements Snapshotter {

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
    }

    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId, List<String> snapshotSelectColumns) {
        String query = snapshotSelectColumns.stream()
                .collect(Collectors.joining(", ", "SELECT ", " FROM ONLY " + tableId.toDoubleQuotedString()));

        return Optional.of(query);
    }

    @Override
    public Optional<String> snapshotTableLockingStatement(Duration lockTimeout, Set<TableId> tableIds) {
        return Optional.empty();
    }

    @Override
    public String snapshotTransactionIsolationLevelStatement(SlotCreationResult newSlotInfo) {
        if (newSlotInfo != null) {
            String snapSet = String.format("SET TRANSACTION SNAPSHOT '%s';", newSlotInfo.snapshotName());
            return "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \n" + snapSet;
        }
        return Snapshotter.super.snapshotTransactionIsolationLevelStatement(newSlotInfo);
    }
}
