/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import java.util.Map;
import java.util.Optional;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.relational.TableId;

public abstract class QueryingSnapshotter implements Snapshotter {

    private Map<TableId, String> snapshotOverrides;

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        this.snapshotOverrides = config.getSnapshotSelectOverridesByTable();
    }

    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId) {
        if (snapshotOverrides.containsKey(tableId)) {
            return Optional.of(snapshotOverrides.get(tableId));
        }
        else {
            // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
            StringBuilder q = new StringBuilder();
            q.append("SELECT * FROM ");
            q.append(tableId.toDoubleQuotedString());
            return Optional.of(q.toString());
        }
    }


}
