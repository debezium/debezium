/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Default implementation of Heartbeat
 *
 * @author Jiri Pechanec
 *
 */
class HeartbeatImpl implements Heartbeat {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatImpl.class);
    private static final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);

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
    private static Schema KEY_SCHEMA = SchemaBuilder.struct()
                                                    .name(schemaNameAdjuster.adjust("io.debezium.connector.common.ServerNameKey"))
                                                    .field(SERVER_NAME_KEY,Schema.STRING_SCHEMA)
                                                    .build();

    private final String topicName;
    private final Duration heartbeatInterval;
    private final String key;

    private volatile Timer heartbeatTimeout;

    HeartbeatImpl(Configuration configuration, String topicName, String key) {
        this.topicName = topicName;
        this.key = key;

        heartbeatInterval = configuration.getDuration(HeartbeatImpl.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS);
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
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer)
            throws InterruptedException {
        LOGGER.debug("Generating heartbeat event");
        consumer.accept(heartbeatRecord(partition, offset));
    }

    /**
     * Produce a key struct based on the server name and KEY_SCHEMA
     *
     */
    private Struct serverNameKey(String serverName){
        Struct result = new Struct(KEY_SCHEMA);
        result.put(SERVER_NAME_KEY, serverName);
        return result;
    }

    /**
     * Produce an empty record to the heartbeat topic.
     *
     */
    private SourceRecord heartbeatRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) {
        final Integer partition = 0;

        return new SourceRecord(sourcePartition, sourceOffset,
                topicName, partition, KEY_SCHEMA, serverNameKey(key), null, null);
    }

    private Timer resetHeartbeat() {
        return Threads.timer(Clock.SYSTEM, heartbeatInterval);
    }
}
