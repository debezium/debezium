/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * This is a small class used in PostgresConnectorIT to test a custom snapshot
 *
 * It is tightly coupled to the test there, but needs to be placed here in order
 * to allow for class loading to work
 */
public class CustomTestSnapshot implements Snapshotter<OffsetContext> {

    private boolean hasState;

    @Override
    public void configure(Properties properties, OffsetContext offsetContext) {
        hasState = (offsetContext != null);
    }

    @Override
    public boolean shouldSnapshot() {
        return true;
    }

    @Override
    public boolean includeSchema() {
        return true;
    }

    @Override
    public boolean includeData() {
        return true;
    }

    @Override
    public boolean shouldStream() {
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
    public Optional<String> buildSnapshotQuery(DataCollectionId id, List<String> snapshotSelectColumns, char quotingChar) {
        if (id instanceof TableId) {
            TableId tableId = (TableId) id;
            if (!hasState && tableId.schema().equals("s2")) {
                return Optional.empty();
            }
            else {
                String query = snapshotSelectColumns.stream()
                        .collect(Collectors.joining(", ", "SELECT ", " FROM " + tableId.toQuotedString(quotingChar)));

                return Optional.of(query);
            }
        }
        return Optional.empty();
    }
}
