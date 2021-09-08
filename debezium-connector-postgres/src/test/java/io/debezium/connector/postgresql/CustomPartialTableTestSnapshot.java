/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.List;
import java.util.Optional;

import io.debezium.relational.TableId;

public class CustomPartialTableTestSnapshot extends CustomStartFromStreamingTestSnapshot {
    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId, List<String> snapshotSelectColumns) {
        if (tableId.schema().equals("s1") && tableId.table().equals("a")) {
            return super.buildSnapshotQuery(tableId, snapshotSelectColumns);
        }

        return Optional.empty();
    }
}
