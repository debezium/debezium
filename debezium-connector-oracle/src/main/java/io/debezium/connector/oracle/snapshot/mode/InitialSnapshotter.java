/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.mode;

import java.util.Map;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.snapshot.mode.HistorizedSnapshotter;

public class InitialSnapshotter extends HistorizedSnapshotter {

    @Override
    public String name() {
        return OracleConnectorConfig.SnapshotMode.INITIAL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    protected boolean shouldSnapshotWhenNoOffset() {
        return true;
    }

    @Override
    protected boolean shouldSnapshotSchemaWhenNoOffset() {
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
