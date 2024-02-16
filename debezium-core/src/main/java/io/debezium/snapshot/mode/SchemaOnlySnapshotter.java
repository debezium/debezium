/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

import java.util.Map;

import io.debezium.bean.StandardBeanNames;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.spi.snapshot.Snapshotter;

public class SchemaOnlySnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    @Override
    public String name() {
        return "schema_only";
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {

        return false;
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {

        final HistorizedRelationalDatabaseSchema databaseSchema = beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, HistorizedRelationalDatabaseSchema.class);

        if (offsetExists && !snapshotInProgress) {
            return databaseSchema.isStorageInitializationExecuted();
        }

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
}
