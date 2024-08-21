/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.debezium.DebeziumException;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.source.EngineSourceTask;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.channels.process.InProcessSignalChannel;

public class AsyncEngineSignaler implements DebeziumEngine.Signaler {
    private AsyncEmbeddedEngine<?> engine;
    private List<InProcessSignalChannel> channels;

    public AsyncEngineSignaler(AsyncEmbeddedEngine<?> engine) {
        this.engine = Objects.requireNonNull(engine);
    }
    
    @Override
    public void signal(DebeziumEngine.Signal signal) {
        if (channels == null) {
            channels = engine.tasks().stream()
                    .map(EngineSourceTask::debeziumConnectTask)
                    .flatMap(Optional::stream)
                    .map(task -> task.getSignalChannel(InProcessSignalChannel.class))
                    .flatMap(Optional::stream)
                    .toList();
        }
        var sr = new SignalRecord(signal);
        channels.forEach(channel -> channel.signal(sr));
    }
}
