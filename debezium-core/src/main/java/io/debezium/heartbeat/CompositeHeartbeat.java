/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.heartbeat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.function.BlockingConsumer;

class CompositeHeartbeat implements Heartbeat {
    private final List<Heartbeat> heartbeats;

    CompositeHeartbeat(List<Heartbeat> heartbeats) {
        this.heartbeats = heartbeats;
    }

    @Override
    public void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.heartbeat(partition, offset, consumer);
        }
    }

    @Override
    public void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.heartbeat(partition, offsetProducer, consumer);
        }
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.heartbeat(partition, offset, consumer);
        }
    }

    @Override
    public boolean isEnabled() {
        return heartbeats.stream().anyMatch(Heartbeat::isEnabled);
    }

    @Override
    public void close() {
        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.close();
        }
    }
}
