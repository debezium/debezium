/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.Optional;

import org.apache.kafka.connect.source.SourceTask;

import io.debezium.connector.common.BaseSourceTask;

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

    @SuppressWarnings("unchecked")
    public Optional<BaseSourceTask<?, ?>> debeziumConnectTask() {
        return Optional.of(connectTask)
                .filter(BaseSourceTask.class::isInstance)
                .map(BaseSourceTask.class::cast);
    }
}
