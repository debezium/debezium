/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.mode;

import io.debezium.connector.oracle.OracleConnectorConfig;

public class SchemaRecoveryOnlySnapshotter extends SchemaOnlySnapshotter {

    @Override
    public String name() {
        return OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY.getValue();
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return true;
    }

}
