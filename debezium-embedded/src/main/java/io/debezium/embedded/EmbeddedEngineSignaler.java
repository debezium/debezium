/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.List;

import io.debezium.engine.DebeziumEngine;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.channels.process.SignalChannelWriter;

public class EmbeddedEngineSignaler implements DebeziumEngine.Signaler {
    private final List<? extends SignalChannelWriter> channels;

    public EmbeddedEngineSignaler(List<? extends SignalChannelWriter> channels) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("At least one channel must be provided");
        }
        this.channels = channels;
    }

    @Override
    public void signal(DebeziumEngine.Signal signal) {
        var sr = new SignalRecord(signal);
        channels.forEach(channel -> channel.signal(sr));
    }
}
