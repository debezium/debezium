/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class QueryingSnapshotter implements Snapshotter {

    private PostgresConnectorConfig config;
    private Map<TableId, String> snapshotOverrides;

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        this.config = config;
        this.snapshotOverrides = getSnapshotSelectOverridesByTable();
    }

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

    /**
     * Returns any SELECT overrides, if present.
     */
    private Map<TableId, String> getSnapshotSelectOverridesByTable() {
        String tableList = config.snapshotSelectOverrides();

        if (tableList == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();

        for (String table : tableList.split(",")) {
            snapshotSelectOverridesByTable.put(
                    TableId.parse(table),
                    config.snapshotSelectOverrideForTable(table)
            );
        }

        return snapshotSelectOverridesByTable;
    }
}
