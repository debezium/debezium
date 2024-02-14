/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot.mode;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.spi.snapshot.Snapshotter;

public class InitialSnapshotter implements Snapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(InitialSnapshotter.class);

    @Override
    public String name() {
        return PostgresConnectorConfig.SnapshotMode.INITIAL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshot(boolean offsetExists, boolean snapshotInProgress) {

        if (!offsetExists) {
            LOGGER.info("Taking initial snapshot for new datasource");
            return true;
        }
        else if (snapshotInProgress) {
            LOGGER.info("Found previous incomplete snapshot");
            return true;
        }
        else {
            LOGGER.info(
                    "Previous snapshot has completed successfully, streaming logical changes from last known position");
            return false;
        }
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
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
        return false;
    }
}
