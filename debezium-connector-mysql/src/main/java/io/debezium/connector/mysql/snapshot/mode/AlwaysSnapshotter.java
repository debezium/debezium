/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.spi.snapshot.Snapshotter;

public class AlwaysSnapshotter implements Snapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(AlwaysSnapshotter.class);

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.ALWAYS.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

    }

    @Override
    public boolean shouldSnapshot() {
        // for ALWAYS snapshot mode don't use exiting offset to have up-to-date SCN
        LOGGER.info("Snapshot mode is set to ALWAYS, not checking exiting offset.");
        return true;
    }

    @Override
    public boolean shouldSnapshotSchema() {
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
    public boolean shouldSnapshotOnDataError() { // TODO check with DBZ-7308
        return false;
    }
}
