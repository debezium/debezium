/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.custom.snapshotter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * This is a small class used in PostgresConnectorIT to test a custom snapshot
 *
 * It is tightly coupled to the test there, but needs to be placed here in order
 * to allow for class loading to work
 */
public class CustomTestSnapshot extends SelectAllSnapshotQuery implements Snapshotter, BeanRegistryAware {

    private boolean hasState;

    @Override
    public String name() {
        return CustomTestSnapshot.class.getName();
    }

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {

        Offsets<PostgresPartition, PostgresOffsetContext> postgresoffsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        hasState = postgresoffsets.getTheOnlyOffset() != null;
    }

    @Override
    public boolean shouldSnapshot(boolean offsetExists, boolean snapshotInProgress) {
        return true;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
        return false;
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
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        if (!hasState && tableId.contains("s2")) {
            return Optional.empty();
        }
        else {
            String query = snapshotSelectColumns.stream()
                    .collect(Collectors.joining(", ", "SELECT ", " FROM " + tableId));

            return Optional.of(query);
        }
    }
}
