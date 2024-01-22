/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot.mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresConnectorConfig;

public class InitialOnlySnapshotter extends InitialSnapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(InitialOnlySnapshotter.class);

    @Override
    public String name() {
        return PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue();
    }

    @Override
    public boolean shouldStream() {
        return false;
    }

    @Override
    public boolean shouldSnapshot() {

        if (!offsetContextExists) {
            LOGGER.info("Taking initial snapshot for new datasource");
            return true;
        }
        else if (isSnapshotInProgress) {
            LOGGER.info("Found previous incomplete snapshot");
            return true;
        }
        else {
            LOGGER.info("Previous initial snapshot completed, no snapshot will be performed");
            return false;
        }
    }
}
