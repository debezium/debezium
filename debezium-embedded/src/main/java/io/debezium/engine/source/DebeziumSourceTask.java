/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import io.debezium.common.annotation.Incubating;

/**
 * {@link DebeziumSourceTask} is a self-contained unit of work created and managed by {@link DebeziumSourceConnector}.
 * In most of the cases the task implementation does actual mining of the changes from the specified resource or its
 * part and further processing of these change records.
 *
 * @author vjuranek
 */
@Incubating
public interface DebeziumSourceTask {
    /**
     * Returns the {@link DebeziumSourceTaskContext} for this DebeziumSourceTask.
     * @return the DebeziumSourceTaskContext for this task
     */
    DebeziumSourceTaskContext context();
}
