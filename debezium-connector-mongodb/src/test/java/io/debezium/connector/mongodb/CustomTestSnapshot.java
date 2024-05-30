/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.spi.snapshot.Snapshotter;

public class CustomTestSnapshot implements Snapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomTestSnapshot.class);

    @Override
    public String name() {
        return CustomTestSnapshot.class.getName();
    }

    @Override
    public void configure(Map<String, ?> properties) {
    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
        LOGGER.info("Should snapshot data {}", true);
        return true;
    }

    @Override
    public boolean shouldStream() {
        LOGGER.info("Should stream {}", false);
        return false;
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

}
