/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.signal.channels.process.SignalChannelWriter;

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

    @Override
    public void start(Map<String, String> config) {
        connectTask.start(config);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return connectTask.poll();
    }

    @Override
    public void stop() {
        connectTask.stop();
    }

    @Override
    public void commit() throws InterruptedException {
        connectTask.commit();
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        connectTask.commitRecord(record, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<? extends SignalChannelWriter> signalChannelWriter() {
        return Optional.of(connectTask)
                .filter(BaseSourceTask.class::isInstance)
                .map(BaseSourceTask.class::cast)
                .flatMap(BaseSourceTask::getAvailableSignalChannelWriter);
    }

    public SourceTask connectTask() {
        return connectTask;
    }
}
