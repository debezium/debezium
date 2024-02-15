/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

import java.util.Map;

import io.debezium.bean.StandardBeanNames;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.snapshot.Snapshotter;

public class WhenNeededSnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    @Override
    public String name() {
        return "when_needed";
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public boolean shouldSnapshot(boolean offsetExists, boolean snapshotInProgress) {

        return !offsetExists || snapshotInProgress;
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {

        final DatabaseSchema databaseSchema = beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, DatabaseSchema.class);

        if (!databaseSchema.isHistorized()) {
            return false;
        }

        final HistorizedRelationalDatabaseSchema historizedRelationalDatabaseSchema = (HistorizedRelationalDatabaseSchema) databaseSchema;
        if (offsetExists && !snapshotInProgress) {
            return historizedRelationalDatabaseSchema.isStorageInitializationExecuted();
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
        return true;
    }

}
