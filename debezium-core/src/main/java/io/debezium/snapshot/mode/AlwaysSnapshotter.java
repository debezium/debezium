/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.spi.snapshot.Snapshotter;

public class AlwaysSnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlwaysSnapshotter.class);

    @Override
    public String name() {
        return "always";
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
        // for ALWAYS snapshot mode don't use exiting offset to have up-to-date SCN
        LOGGER.info("Snapshot mode is set to ALWAYS, not checking exiting offset.");
        return true;
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {

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
