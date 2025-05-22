/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.List;

import io.debezium.connector.common.BaseSourceTask;

public interface LineageEmitter {

    void emit(BaseSourceTask.State state);

    void emit(BaseSourceTask.State state, Throwable t);

    void emit(BaseSourceTask.State state, List<DataCollectionMetadata> inputDatasetMetadata);

    void emit(BaseSourceTask.State state, List<DataCollectionMetadata> inputDatasetMetadata, Throwable t);
}
