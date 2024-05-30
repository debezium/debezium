/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

public class InitialOnlySnapshotter extends InitialSnapshotter {

    @Override
    public String name() {
        return "initial_only";
    }

    @Override
    public boolean shouldStream() {
        return false;
    }

}
