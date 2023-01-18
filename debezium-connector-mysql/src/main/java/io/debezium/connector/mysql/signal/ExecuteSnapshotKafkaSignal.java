/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import java.util.List;
import java.util.Optional;

public class ExecuteSnapshotKafkaSignal implements KafkaSignal {
    private final List<String> dataCollections;
    private final long signalOffset;
    private final Optional<String> additionalCondition;
    private final Optional<String> surrogateKey;

    public ExecuteSnapshotKafkaSignal(List<String> dataCollections, long signalOffset, Optional<String> additionalCondition, Optional<String> surrogateKey) {
        this.dataCollections = dataCollections;
        this.signalOffset = signalOffset;
        this.additionalCondition = additionalCondition;
        this.surrogateKey = surrogateKey;
    }

    public List<String> getDataCollections() {
        return dataCollections;
    }

    public long getSignalOffset() {
        return signalOffset;
    }

    public Optional<String> getAdditionalCondition() {
        return additionalCondition;
    }

    public Optional<String> getSurrogateKey() {
        return surrogateKey;
    }
}
