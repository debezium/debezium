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
import io.debezium.connector.postgresql.YugabyteDBServer;
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
                .collect(Collectors.joining(", ", "SELECT ", " FROM " + tableId.toDoubleQuotedString()));

        return Optional.of(query);
    }

    @Override
    public Optional<String> snapshotTableLockingStatement(Duration lockTimeout, Set<TableId> tableIds) {
        return Optional.empty();
    }

    @Override
    public String snapshotTransactionIsolationLevelStatement(SlotCreationResult newSlotInfo, boolean isOnDemand) {
        if (newSlotInfo != null && !isOnDemand) {
            // YB Note: This is a temporary change. The consistent snapshot time is set as the upper
            // bound of the maximum time on the nodes of the Universe and could be 0.5 seconds ahead
            // of the time on some tserver nodes. The "SET LOCAL yb_read_time" will return
            // an error if the time to be set is in the future. The sleep for 1 second to ensure
            // that this does not happen.
            //
            // Most likely this will be fixed on the YB server side. At that point, this sleep can
            // be removed from here.
            if (YugabyteDBServer.isEnabled()) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    throw new RuntimeException("Exception while waiting", e);
                }

                return String.format("SET LOCAL yb_read_time TO '%s ht'", newSlotInfo.snapshotName());
            }

            /*
             * For an on demand blocking snapshot we don't need to reuse
             * the same snapshot from the existing exported transaction as for the initial snapshot.
             */
             String snapSet = String.format("SET TRANSACTION SNAPSHOT '%s';", newSlotInfo.snapshotName());
             return "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \n" + snapSet;
        }
        return Snapshotter.super.snapshotTransactionIsolationLevelStatement(newSlotInfo, isOnDemand);
    }
}
