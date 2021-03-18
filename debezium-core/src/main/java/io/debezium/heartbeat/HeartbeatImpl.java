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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Default implementation of Heartbeat
 *
 */
class HeartbeatImpl implements Heartbeat {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatImpl.class);
    private static final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

    /**
     * Default length of interval in which connector generates periodically
     * heartbeat messages. A size of 0 disables heartbeat.
     */
    static final int DEFAULT_HEARTBEAT_INTERVAL = 0;

    /**
     * Default prefix for names of heartbeat topics
     */
    static final String DEFAULT_HEARTBEAT_TOPICS_PREFIX = "__debezium-heartbeat";

    private static final String SERVER_NAME_KEY = "serverName";

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.common.ServerNameKey"))
            .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
            .build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.common.Heartbeat"))
            .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
            .build();

    private final String topicName;
    private final Duration heartbeatInterval;
    private final String key;

    private volatile Timer heartbeatTimeout;

    HeartbeatImpl(Duration heartbeatInterval, String topicName, String key) {
        this.topicName = topicName;
        this.key = key;
        this.heartbeatInterval = heartbeatInterval;
        heartbeatTimeout = resetHeartbeat();
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
    public boolean isEnabled() {
        return true;
    }

    /**
     * Produce a key struct based on the server name and KEY_SCHEMA
     *
     */
    private Struct serverNameKey(String serverName) {
        Struct result = new Struct(KEY_SCHEMA);
        result.put(SERVER_NAME_KEY, serverName);
        return result;
    }

    /**
     * Produce a value struct containing the timestamp
     *
     */
    private Struct messageValue() {
        Struct result = new Struct(VALUE_SCHEMA);
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
                topicName, partition, KEY_SCHEMA, serverNameKey(key), VALUE_SCHEMA, messageValue());
    }

    private Timer resetHeartbeat() {
        return Threads.timer(Clock.SYSTEM, heartbeatInterval);
    }
}
