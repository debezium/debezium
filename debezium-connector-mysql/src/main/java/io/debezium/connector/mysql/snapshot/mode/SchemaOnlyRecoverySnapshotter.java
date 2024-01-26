/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import io.debezium.connector.mysql.MySqlConnectorConfig;

public class SchemaOnlyRecoverySnapshotter extends SchemaOnlySnapshotter {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY.getValue();
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return true;
    }

}
