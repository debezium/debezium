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
 * Composite Heartbeat solutions (Queue, Database...) {@link Heartbeat}. It executes the heartbeats in chain
 *
 * @author gpanice
 */
public class CompositeHeartbeat implements Heartbeat, Heartbeat.ScheduledHeartbeat {
    private final ScheduledHeartbeat scheduledHeartbeat;
    private final List<Heartbeat> heartbeats;

    public CompositeHeartbeat(ScheduledHeartbeat scheduledHeartbeat, Heartbeat... heartbeat) {
        this.scheduledHeartbeat = scheduledHeartbeat;
        this.heartbeats = Arrays.stream(heartbeat).toList();
    }

    public CompositeHeartbeat(ScheduledHeartbeat scheduledHeartbeat, List<Heartbeat> heartbeats) {
        this.scheduledHeartbeat = scheduledHeartbeat;
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
    public void emitWithDelay(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        scheduledHeartbeat.emitWithDelay(partition, offset);

        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.emit(partition, offset);
        }
    }

    @Override
    public void emit(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        scheduledHeartbeat.emit(partition, offset);

        for (Heartbeat heartbeat : heartbeats) {
            heartbeat.emit(partition, offset);
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
