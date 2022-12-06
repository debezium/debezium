/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.snapshot.AlwaysSnapshotter;

public class CustomLifecycleHookTestSnapshot extends AlwaysSnapshotter {

    private static final String INSERT_SNAPSHOT_COMPLETE_STATE = "INSERT INTO s1.lifecycle_state (hook, state) " +
            "VALUES ('snapshotComplete', 'complete');";

    @Override
    public void snapshotCompleted() {
        try (PostgresConnection connection = TestHelper.create()) {
            TestHelper.execute(connection, INSERT_SNAPSHOT_COMPLETE_STATE);
        }
    }
}
