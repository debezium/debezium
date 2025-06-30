/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Default implementation of Heartbeat
 *
 */
public class HeartbeatImpl implements Heartbeat.ScheduledHeartbeat, Heartbeat {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatImpl.class);

    /**
     * Default length of interval in which connector generates periodically
     * heartbeat messages. A size of 0 disables heartbeat.
     */
    static final int DEFAULT_HEARTBEAT_INTERVAL = 0;

    /**
     * Default prefix for names of heartbeat topics
     */
    static final String DEFAULT_HEARTBEAT_TOPICS_PREFIX = "__debezium-heartbeat";

    public static final String SERVER_NAME_KEY = "serverName";

    private final String topicName;
    private final Duration heartbeatInterval;
    private final String key;

    private final Schema keySchema;
    private final Schema valueSchema;
    private final Struct serverNameKey;
    private final ChangeEventQueue<DataChangeEvent> queue;

    private volatile Timer heartbeatTimeout;

    public HeartbeatImpl(Duration heartbeatInterval, String topicName, String key, SchemaNameAdjuster schemaNameAdjuster) {
        this.topicName = topicName;
        this.key = key;
        this.heartbeatInterval = heartbeatInterval;
        this.keySchema = SchemaFactory.get().heartbeatKeySchema(schemaNameAdjuster);
        this.valueSchema = SchemaFactory.get().heartbeatValueSchema(schemaNameAdjuster);
        this.heartbeatTimeout = resetHeartbeat();
        this.serverNameKey = serverNameKey();
        this.queue = null;
    }

    public HeartbeatImpl(Duration heartbeatInterval, String topicName, String key, SchemaNameAdjuster schemaNameAdjuster, ChangeEventQueue<DataChangeEvent> queue) {
        this.topicName = topicName;
        this.heartbeatInterval = heartbeatInterval;
        this.key = key;
        this.keySchema = SchemaFactory.get().heartbeatKeySchema(schemaNameAdjuster);
        this.valueSchema = SchemaFactory.get().heartbeatValueSchema(schemaNameAdjuster);
        this.serverNameKey = serverNameKey();
        this.queue = queue;
        this.heartbeatTimeout = resetHeartbeat();
    }

    @Override
    public void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        if (heartbeatTimeout.expired()) {
            forcedBeat(partition, offset, consumer);
            heartbeatTimeout = resetHeartbeat();
        }
    }

    @Override
    public void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        if (heartbeatTimeout.expired()) {
            forcedBeat(partition, offsetProducer.offset(), consumer);
            heartbeatTimeout = resetHeartbeat();
        }
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer)
            throws InterruptedException {
        LOGGER.debug("Generating heartbeat event");
        if (offset == null || offset.isEmpty()) {
            // Do not send heartbeat message if no offset is available yet
            return;
        }
        consumer.accept(heartbeatRecord(partition, offset));
    }

    @Override
    public void emitWithDelay(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        if (heartbeatTimeout.expired()) {
            emit(partition, offset);
            heartbeatTimeout = resetHeartbeat();
        }
    }

    @Override
    public void emit(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        if (queue == null) {
            throw new IllegalArgumentException("new heartbeat API should be used with the recommended constructor");
        }
        LOGGER.debug("Generating heartbeat event");
        if (offset == null || offset.getOffset() == null || offset.getOffset().isEmpty()) {
            return;
        }

        this.queue.enqueue(new DataChangeEvent(new SourceRecord(partition,
                offset.getOffset(),
                topicName,
                0,
                keySchema,
                serverNameKey,
                valueSchema,
                messageValue())));
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    private Struct serverNameKey() {
        Struct result = new Struct(keySchema);
        result.put(SERVER_NAME_KEY, key);
        return result;
    }

    /**
     * Produce a value struct containing the timestamp
     *
     */
    private Struct messageValue() {
        Struct result = new Struct(valueSchema);
        result.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now().toEpochMilli());
        return result;
    }

    /**
     * Produce an empty record to the heartbeat topic.
     *
     */
    private SourceRecord heartbeatRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) {
        final Integer partition = 0;

        return new SourceRecord(sourcePartition, sourceOffset,
                topicName, partition, keySchema, serverNameKey, valueSchema, messageValue());
    }

    private Timer resetHeartbeat() {
        return Threads.timer(Clock.SYSTEM, heartbeatInterval);
    }
}
