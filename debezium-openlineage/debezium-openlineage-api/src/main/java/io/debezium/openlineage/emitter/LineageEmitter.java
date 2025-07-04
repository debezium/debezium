/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.emitter;

import java.util.List;

import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.dataset.DatasetMetadata;

public interface LineageEmitter {

    void emit(DebeziumTaskState state);

    void emit(DebeziumTaskState state, Throwable t);

    void emit(DebeziumTaskState state, List<DatasetMetadata> datasetMetadata);

    void emit(DebeziumTaskState state, List<DatasetMetadata> datasetMetadata, Throwable t);
}
