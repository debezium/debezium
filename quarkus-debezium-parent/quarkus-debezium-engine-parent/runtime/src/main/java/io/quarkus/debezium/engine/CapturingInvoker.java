/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;

public interface CapturingInvoker {
    void capture(RecordChangeEvent<SourceRecord> event);

    String getFullyQualifiedTableName();
}
