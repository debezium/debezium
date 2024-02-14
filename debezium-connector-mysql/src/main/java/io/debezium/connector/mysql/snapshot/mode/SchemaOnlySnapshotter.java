/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import java.util.Map;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.snapshot.mode.HistorizedSnapshotter;

public class SchemaOnlySnapshotter extends HistorizedSnapshotter {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    protected boolean shouldSnapshotWhenNoOffset() {
        return false;
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
