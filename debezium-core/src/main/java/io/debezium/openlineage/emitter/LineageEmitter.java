/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.emitter;

import java.util.List;

import io.debezium.connector.common.BaseSourceTask;
import io.debezium.openlineage.dataset.DatasetMetadata;

public interface LineageEmitter {

    void emit(BaseSourceTask.State state);

    void emit(BaseSourceTask.State state, Throwable t);

    void emit(BaseSourceTask.State state, List<DatasetMetadata> datasetMetadata);

    void emit(BaseSourceTask.State state, List<DatasetMetadata> datasetMetadata, Throwable t);
}
