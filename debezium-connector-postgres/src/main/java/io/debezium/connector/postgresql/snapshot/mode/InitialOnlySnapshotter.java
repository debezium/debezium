/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot.mode;

import io.debezium.connector.postgresql.PostgresConnectorConfig;

public class InitialOnlySnapshotter extends InitialSnapshotter {

    @Override
    public String name() {
        return PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue();
    }

    @Override
    public boolean shouldStream() {
        return false;
    }

}
