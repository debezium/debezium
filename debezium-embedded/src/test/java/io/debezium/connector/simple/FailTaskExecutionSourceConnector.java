/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.simple;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.Configuration;

/**
 * A very simple {@link SourceConnector} for testing that fails during task execution, useful for testing the infrastructure to
 * run {@link SourceConnector}s.
 * <p>
 * This connector behaves exactly like {@link SimpleSourceConnector}, except that the last task started will fail before
 * creating the second batch.
 * 
 * @author Randall Hauch
 */
public class FailTaskExecutionSourceConnector extends SimpleSourceConnector {

    public FailTaskExecutionSourceConnector() {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FailTaskExecutionSourceConnector.FailingExecutionTask.class;
    }

    public static class FailingExecutionTask extends SimpleConnectorTask {

        private boolean isLastTask = false;
        
        @Override
        protected void preStart(Configuration config) {
            int taskCount = config.getInteger(TASK_COUNT);
            int taskId = config.getInteger(TASK_ID);
            isLastTask = taskId == taskCount;
        }
        
        @Override
        protected void preBatch(int batchNumber) {
            if (isLastTask && batchNumber == 2) {
                throw new ConnectException("Planned failure upon task execution");
            }
        }
    }
}
