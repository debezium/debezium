/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.spi.snapshot.Snapshotter;

public abstract class HistorizedSnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistorizedSnapshotter.class);
    public static final String OFFSET_FOUND_MSG_PREFIX = "A previous offset indicating a completed snapshot has been found. ";
    public static final String OFFSET_FOUND_MSG_TEMPLATE = OFFSET_FOUND_MSG_PREFIX +
            "Schema will %s if shouldSnapshotOnSchemaError is true for the current snapshot mode.";

    @Override
    public boolean shouldSnapshot() {

        final Offsets offsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        OffsetContext previousOffset = offsets.getTheOnlyOffset();

        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info(OFFSET_FOUND_MSG_PREFIX + "Data will not be snapshotted");
            return false;
        }

        LOGGER.info("No previous offset has been found.");

        return shouldSnapshotWhenNoOffset();
    }

    protected abstract boolean shouldSnapshotWhenNoOffset();

    protected abstract boolean shouldSnapshotSchemaWhenNoOffset();

    @Override
    public boolean shouldSnapshotSchema() {

        final Offsets offsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        final HistorizedRelationalDatabaseSchema databaseSchema = beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, HistorizedRelationalDatabaseSchema.class);
        OffsetContext previousOffset = offsets.getTheOnlyOffset();

        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info(String.format(OFFSET_FOUND_MSG_TEMPLATE, databaseSchema.isStorageInitializationExecuted() ? "be snapshotted" : "not be snapshotted"));
            return databaseSchema.isStorageInitializationExecuted();
        }

        LOGGER.info("No previous offset has been found.");
        return shouldSnapshotSchemaWhenNoOffset();
    }

}
