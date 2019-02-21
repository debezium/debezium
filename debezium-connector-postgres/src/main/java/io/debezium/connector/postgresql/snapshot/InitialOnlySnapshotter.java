/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

public class InitialOnlySnapshotter extends QueryingSnapshotter {
    @Override
    public boolean shouldStream() {
        return false;
    }

    @Override
    public boolean shouldSnapshot() {
        return true;
    }
}
