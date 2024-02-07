/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.spi.snapshot.Snapshotter;

public abstract class OffsetAwareSnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetAwareSnapshotter.class);

    @Override
    public boolean shouldSnapshot() {

        final Offsets offsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        OffsetContext previousOffset = offsets.getTheOnlyOffset();

        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("The previous offset has been found.");
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
        final OracleDatabaseSchema databaseSchema = beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, OracleDatabaseSchema.class);
        OffsetContext previousOffset = offsets.getTheOnlyOffset();

        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("The previous offset has been found.");
            return databaseSchema.isStorageInitializationExecuted();
        }

        LOGGER.info("No previous offset has been found.");
        return shouldSnapshotSchemaWhenNoOffset();
    }
}
