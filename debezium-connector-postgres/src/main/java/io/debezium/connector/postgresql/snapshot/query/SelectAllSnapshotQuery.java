/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.snapshot.spi.SnapshotQuery;

public class SelectAllSnapshotQuery implements SnapshotQuery {

    @Override
    public String name() {
        return PostgresConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        return Optional.of(snapshotSelectColumns.stream()
                .collect(Collectors.joining(", ", "SELECT ", " FROM " + tableId)));
    }
}
