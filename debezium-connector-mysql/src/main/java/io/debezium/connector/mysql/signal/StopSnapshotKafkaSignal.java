/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import java.util.List;

/**
 * A {@link KafkaSignal} implementation to stop a current executing snapshot.
 *
 * @author Chris Cranford
 */
public class StopSnapshotKafkaSignal implements KafkaSignal {
    private final List<String> dataCollections;
    private final long signalOffset;

    public StopSnapshotKafkaSignal(List<String> dataCollections, long signalOffset) {
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
