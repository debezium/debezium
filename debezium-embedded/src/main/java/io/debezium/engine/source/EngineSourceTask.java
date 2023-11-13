/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import org.apache.kafka.connect.source.SourceTask;

/**
 * Implementation of {@link DebeziumSourceTask} which currently serves only as a wrapper
 * around Kafka Connect {@link SourceTask}.
 *
 * @author vjuranek
 */
public class EngineSourceTask implements DebeziumSourceTask {

    private final SourceTask connectTask;
    private final DebeziumSourceTaskContext context;

    public EngineSourceTask(final SourceTask connectTask, final DebeziumSourceTaskContext context) {
        this.connectTask = connectTask;
        this.context = context;
    }

    @Override
    public DebeziumSourceTaskContext context() {
        return context;
    }

    public SourceTask connectTask() {
        return connectTask;
    }
}