/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Optional;

import io.debezium.DebeziumException;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.source.EngineSourceTask;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.channels.process.InProcessSignalChannel;

public class AsyncEngineSignaler implements DebeziumEngine.Signaler<SignalRecord> {
    private AsyncEmbeddedEngine<?> engine;

    @Override
    public <E extends DebeziumEngine<?>> void init(E engine) {
        if (!(engine instanceof AsyncEmbeddedEngine)) {
            throw new DebeziumException("AsyncEngineSignaler can only be used with AsyncEmbeddedEngine");
        }
        this.engine = (AsyncEmbeddedEngine<?>) engine;
    }

    @Override
    public void signal(SignalRecord signal) {
        engine.tasks().stream()
                .map(EngineSourceTask::debeziumConnectTask)
                .flatMap(Optional::stream)
                .map(task -> task.getSignalChannel(InProcessSignalChannel.class))
                .flatMap(Optional::stream)
                .forEach(channel -> channel.signal(signal));
    }
}
