/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.List;
import java.util.Optional;

import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

public class CustomPartialTableTestSnapshot extends CustomStartFromStreamingTestSnapshot {
    @Override
    public Optional<String> buildSnapshotQuery(DataCollectionId id, List<String> snapshotSelectColumns, char quotingChar) {
        if (id instanceof TableId) {
            TableId tableId = (TableId) id;
            if (tableId.schema().equals("s1") && tableId.table().equals("a")) {
                return super.buildSnapshotQuery(tableId, snapshotSelectColumns, quotingChar);
            }
        }
        return Optional.empty();
    }
}
