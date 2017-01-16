/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.simple;

import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;

import io.debezium.config.Configuration;

/**
 * A very simple {@link SourceConnector} for testing that fails upon task startup, useful for testing the infrastructure to run
 * {@link SourceConnector}s.
 * <p>
 * This connector behaves exactly like {@link SimpleSourceConnector}, except that the last task started will always fail
 * during {@link SourceTask#start(Map) startup}.
 * 
 * @author Randall Hauch
 */
public class FailOnTaskStartupSourceConnector extends SimpleSourceConnector {

    public FailOnTaskStartupSourceConnector() {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FailOnTaskStartupSourceConnector.FailingStartupTask.class;
    }

    public static class FailingStartupTask extends SimpleConnectorTask {
        @Override
        protected void preStart(Configuration config) {
            int taskCount = config.getInteger(TASK_COUNT);
            int taskId = config.getInteger(TASK_ID);
            if (taskId == taskCount) {
                // This is the last task, so fail ...
                throw new ConnectException("Planned failure upon startup");
            }
        }
    }
}
