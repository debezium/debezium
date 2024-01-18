/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.snapshot.AlwaysSnapshotter;

public class CustomLifecycleHookTestSnapshot extends AlwaysSnapshotter {

    private static final String INSERT_SNAPSHOT_COMPLETE_STATE = "INSERT INTO s1.lifecycle_state (hook, state) " +
            "VALUES ('snapshotComplete', 'complete');";

    private static final String INSERT_SNAPSHOT_ABORTED_STATE = "INSERT INTO s1.lifecycle_state (hook, state) " +
            "VALUES ('snapshotComplete', 'aborted');";

    @Override
    public void snapshotCompleted() {
        TestHelper.execute(INSERT_SNAPSHOT_COMPLETE_STATE);
    }

    @Override
    public void snapshotAborted() {
        TestHelper.execute(INSERT_SNAPSHOT_ABORTED_STATE);
    }
}
