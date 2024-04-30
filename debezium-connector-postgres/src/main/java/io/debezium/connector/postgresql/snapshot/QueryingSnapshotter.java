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

    private SlotState slotState;

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        if (YugabyteDBServer.isEnabled()) {
            this.slotState = slotState;
        }
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

        if (YugabyteDBServer.isEnabled() && !isOnDemand) {
            // In case of YB, the consistent snapshot is performed as follows -
            // 1) If connector created the slot, then the snapshotName returned as part of the CREATE_REPLICATION_SLOT
            //    command will have the hybrid time as of which the snapshot query is to be run
            // 2) If slot already exists, then the snapshot query will be run as of the hybrid time corresponding to the
            //    restart_lsn. This information is available in the pg_replication_slots view
            // For YB, one of these 2 cases will hold
            // In both cases, streaming will continue from confirmed_flush_lsn

            // YB Note: This is a temporary change. The consistent snapshot time is set as the upper
            // bound of the maximum time on the nodes of the Universe and could be 0.5 seconds ahead
            // of the time on some tserver nodes. The "SET LOCAL yb_read_time" will return
            // an error if the time to be set is in the future. The sleep for 1 second to ensure
            // that this does not happen.
            //
            // Most likely this will be fixed on the YB server side. At that point, this sleep can
            // be removed from here.
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new RuntimeException("Exception while waiting", e);
            }

            if (newSlotInfo != null) {
                return String.format("SET LOCAL yb_read_time TO '%s ht'", newSlotInfo.snapshotName());
            }
            else {
                return String.format("SET LOCAL yb_read_time TO '%s ht'", slotState.slotRestartCommitHT());
            }
        }

        // PG case
        if (newSlotInfo != null && !isOnDemand) {
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
