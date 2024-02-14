/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import java.util.Map;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.snapshot.mode.BeanAwareSnapshotter;
import io.debezium.spi.snapshot.Snapshotter;

public class NeverSnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.NEVER.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public boolean shouldSnapshot(boolean offsetExists, boolean snapshotInProgress) {
        return false;
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

}
