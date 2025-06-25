/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.heartbeat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Composite Heartbeat {@link Heartbeat}. It executes the heartbeats in chain
 *
 * @author gpanice
 */
public class CompositeHeartbeat implements Heartbeat {
    private final List<Heartbeat> heartbeats;

    public CompositeHeartbeat(Heartbeat... heartbeat) {
        this.heartbeats = Arrays.stream(heartbeat).toList();
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
    public void heartbeat(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.heartbeat(partition, offset);
        }
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.forcedBeat(partition, offset);
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
