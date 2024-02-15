/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

public class SchemaOnlyRecoverySnapshotter extends SchemaOnlySnapshotter {

    @Override
    public String name() {
        return "schema_only_recovery";
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return true;
    }

}
