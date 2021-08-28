/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import java.util.List;

public class ExecuteSnapshotKafkaSignal {
    private final List<String> dataCollections;
    private final long signalOffset;

    public ExecuteSnapshotKafkaSignal(List<String> dataCollections, long signalOffset) {
        this.dataCollections = dataCollections;
        this.signalOffset = signalOffset;
    }

    public List<String> getDataCollections() {
        return dataCollections;
    }

    public long getSignalOffset() {
        return signalOffset;
    }
}
