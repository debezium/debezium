/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.source.SourceTask;

public class TaskExecutionContext {

    private final int taskId;
    private final SourceTask sourceTask;

    public TaskExecutionContext(int taskId, SourceTask sourceTask) {
        this.taskId = taskId;
        this.sourceTask = sourceTask;

    }
}
